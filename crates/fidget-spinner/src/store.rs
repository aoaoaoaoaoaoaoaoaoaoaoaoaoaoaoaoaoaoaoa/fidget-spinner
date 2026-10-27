//! The frontier ledger: trebuchet's store under Fidget Spinner's ontology, and
//! the in-place migration of FS 3 stores.

use std::{
    io,
    path::{Path, PathBuf},
};

use fidget_spinner_core::{Frontier, Op, Patched, Refusal, Stamp, Warning, bar_reserved, legacy};
use serde::Deserialize;
use serde_json::Value as Json;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use trebuchet::ledger::{
    AddressTerms, Applied, ApplyResult, FormatError, Ontology, SqlNames, StoreFormat,
    rusqlite::{self, Transaction, params},
};

pub use trebuchet::ledger::{
    Address, Decision, Entry, Ledger, Listing, ProjectRoot, Standing, StoreError,
};

use crate::mcp::Fault;

/// The store format this build writes.
pub const FORMAT: i64 = 25;
/// FS 4.2's format, which lacks only the late bridge and so promotes unchanged.
const UNBRIDGEABLE: i64 = 24;
/// FS 4.1's format, which also lacks titles and synopses.
const UNTITLED: i64 = 23;
/// FS 4.0's format, which also lacks annotate and so promotes unchanged.
const UNANNOTATED: i64 = 22;
/// FS 3's format, translated in place when a writer first opens it.
const FS3: i64 = 21;
const FRONTIERS: &str = "frontiers";
const PATCHES: &str = "patches";
const FRONTIER: &str = "frontier_id";

pub struct Spinner;
pub type Store = trebuchet::ledger::Store<Spinner>;

impl Ontology for Spinner {
    const ADDRESS_TERMS: AddressTerms = AddressTerms {
        noun: "frontier",
        candidates: "unarchived frontiers",
    };
    type State = Frontier;
    type Op = Op;
    type Warning = (usize, Warning);
    type Refusal = Refusal;
    type Report = Patched;

    fn apply(state: &Frontier, ops: &[Op], stamp: trebuchet::ledger::Stamp) -> ApplyResult<Self> {
        let mut patched = state.patched(ops, stamped(stamp))?;
        let warnings = std::mem::take(&mut patched.warnings)
            .into_iter()
            .flat_map(|(index, warnings)| warnings.into_iter().map(move |w| (index, w)))
            .collect();
        Ok(Applied {
            state: patched.frontier.clone(),
            warnings,
            report: patched,
        })
    }

    fn replay(
        state: &mut Frontier,
        ops: &[Op],
        stamp: trebuchet::ledger::Stamp,
    ) -> Result<(), Refusal> {
        state.replay(ops, stamped(stamp))
    }
}

const fn stamped(stamp: trebuchet::ledger::Stamp) -> Stamp {
    Stamp {
        seq: stamp.seq,
        time: stamp.at,
    }
}

fn names() -> Result<SqlNames, FormatError> {
    SqlNames::new(FRONTIERS, PATCHES, FRONTIER, "patches_frontier_seq")
}

/// The writer's format: reads FS 3 and formats 22 to 25, writes 25.
pub fn format() -> Result<StoreFormat, FormatError> {
    Ok(StoreFormat::new(
        names()?,
        vec![FS3, UNANNOTATED, UNTITLED, UNBRIDGEABLE, FORMAT],
        FORMAT,
    )?
    .with_migration(migrate))
}

/// Open the store for writing, migrating an FS 3 store first.
pub fn open(path: &Path) -> Result<Store, Fault> {
    let mut store = Store::open(path, format()?)?;
    store.promote()?;
    Ok(store)
}

/// Open an existing store for reading. An FS 3 store is refused until a writer
/// migrates it; None means no store exists yet.
pub fn read_only(path: &Path) -> Result<Option<Store>, Fault> {
    Ok(Store::open_read_only(
        path,
        StoreFormat::new(
            names()?,
            vec![UNANNOTATED, UNTITLED, UNBRIDGEABLE, FORMAT],
            FORMAT,
        )?,
    )?)
}

/// Promote formats 22 to 24 unchanged, since 25 only adds annotate, titles,
/// synopses and the late bridge. Translate every FS 3 patch in place, preserving seqs, times, patch
/// boundaries and operation order, and prove that the result replays.
fn migrate(tx: &Transaction<'_>, from: i64, to: i64) -> rusqlite::Result<()> {
    if matches!(from, UNANNOTATED | UNTITLED | UNBRIDGEABLE) && to == FORMAT {
        return Ok(());
    }
    if (from, to) != (FS3, FORMAT) {
        return Err(failure(format!(
            "no migration from store format {from} to {to}"
        )));
    }
    let frontiers = tx
        .prepare(&format!("SELECT id FROM \"{FRONTIERS}\" ORDER BY id"))?
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut select = tx.prepare(&format!(
        "SELECT seq,at,ops FROM \"{PATCHES}\" WHERE \"{FRONTIER}\"=?1 ORDER BY seq"
    ))?;
    let mut update = tx.prepare(&format!("UPDATE \"{PATCHES}\" SET ops=?1 WHERE seq=?2"))?;
    for frontier in frontiers {
        let patches = select
            .query_map([frontier], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let Some(&(through, ..)) = patches.last() else {
            continue;
        };
        let mut translator = legacy::Translator::new(through);
        let mut state = Frontier::default();
        for (seq, at, ops) in patches {
            let fault =
                |error: &dyn std::fmt::Display| failure(format!("FS 3 patch {seq}: {error}"));
            let raw: Vec<Json> = serde_json::from_str(&ops).map_err(|e| fault(&e))?;
            let ops = translator
                .patch(&raw)
                .map_err(|e| fault(&e))?
                .into_iter()
                .map(Op::parse)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| fault(&e))?;
            let time = OffsetDateTime::parse(&at, &Rfc3339).map_err(|e| fault(&e))?;
            state
                .replay(&ops, Stamp { seq, time })
                .map_err(|e| fault(&e))?;
            let ops = serde_json::to_string(&ops).map_err(|e| fault(&e))?;
            update.execute(params![ops, seq])?;
        }
    }
    Ok(())
}

fn failure(message: String) -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ABORT),
        Some(message),
    )
}

pub fn default_path() -> io::Result<PathBuf> {
    dirs::state_dir()
        .map(|p| p.join("fidget-spinner/ledger.sqlite3"))
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no platform state directory"))
}

pub fn project_root(path: &Path) -> io::Result<ProjectRoot> {
    trebuchet::ledger::project_root(path)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportLine {
    #[serde(with = "time::serde::rfc3339")]
    at: OffsetDateTime,
    ops: Vec<Op>,
}

/// Import timestamped `{at, ops}` JSONL into a new frontier, atomically.
pub fn import(path: &Path, address: &Address, source: &str) -> Result<usize, Fault> {
    let patches = source
        .lines()
        .enumerate()
        .map(|(index, line)| {
            let line: ImportLine = serde_json::from_str(line)
                .map_err(|e| Fault::Arguments(format!("line {}: {e}", index + 1)))?;
            if line.ops.is_empty() {
                return Err(Fault::Arguments(format!(
                    "line {}: a patch needs at least one operation",
                    index + 1
                )));
            }
            bar_reserved(&line.ops)
                .map_err(|e| Fault::Arguments(format!("line {}: {e}", index + 1)))?;
            Ok((line.at, line.ops))
        })
        .collect::<Result<Vec<_>, Fault>>()?;
    Ok(open(path)?.import(address, patches)?.receipts.len())
}
