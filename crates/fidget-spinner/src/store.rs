use fidget_spinner_core::{Counts, FindingId, Frontier, Op, Patched, Refusal, Warning};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
};
use time::OffsetDateTime;
use trebuchet::ledger::{Applied, ApplyResult, Ontology, SqlNames, StoreFormat};

pub use trebuchet::ledger::{Address, Decision, Entry, Ledger, Listing, ProjectRoot, StoreError};

pub struct Spinner;
pub type Store = trebuchet::ledger::Store<Spinner>;

#[derive(Debug, Serialize)]
pub struct OpWarnings {
    pub index: usize,
    pub warnings: Vec<Warning>,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub raised: BTreeMap<FindingId, Option<String>>,
    pub cleared: BTreeMap<FindingId, Option<String>>,
    pub before: Counts,
    pub after: Counts,
    pub created: Vec<fidget_spinner_core::Created>,
}

impl Ontology for Spinner {
    const ADDRESS_TERMS: trebuchet::ledger::AddressTerms = trebuchet::ledger::AddressTerms {
        noun: "frontier",
        candidates: "frontiers",
    };
    type State = Frontier;
    type Op = Op;
    type Warning = OpWarnings;
    type Refusal = Refusal;
    type Report = Report;

    fn apply(state: &Frontier, ops: &[Op], at: OffsetDateTime) -> ApplyResult<Self> {
        let Patched {
            frontier,
            warnings,
            raised,
            cleared,
            before,
            after,
            created,
        } = state.patched(ops, at)?;
        Ok(Applied {
            state: frontier,
            warnings: warnings
                .into_iter()
                .map(|(index, warnings)| OpWarnings { index, warnings })
                .collect(),
            report: Report {
                raised,
                cleared,
                before,
                after,
                created,
            },
        })
    }

    fn replay(state: &mut Frontier, ops: &[Op], at: OffsetDateTime) -> Result<(), Refusal> {
        state.replay(ops, at)
    }
}

pub fn format() -> Result<StoreFormat, trebuchet::ledger::FormatError> {
    StoreFormat::new(
        SqlNames::new(
            "frontiers",
            "patches",
            "frontier_id",
            "patches_frontier_seq",
        )?,
        vec![21],
        21,
    )
}

pub fn open(path: &Path) -> Result<Store, crate::mcp::Fault> {
    Ok(Store::open(path, format()?)?)
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
pub struct ImportLine {
    #[serde(with = "time::serde::rfc3339")]
    pub at: OffsetDateTime,
    pub ops: Vec<Op>,
}

pub fn import(path: &Path, address: &Address, source: &str) -> Result<usize, crate::mcp::Fault> {
    let patches: Vec<ImportLine> = source
        .lines()
        .enumerate()
        .map(|(index, line)| {
            serde_json::from_str(line)
                .map_err(|e| crate::mcp::Fault::Arguments(format!("line {}: {e}", index + 1)))
        })
        .collect::<Result<_, _>>()?;
    let imported =
        open(path)?.import(address, patches.into_iter().map(|line| (line.at, line.ops)))?;
    Ok(imported.receipts.len())
}
