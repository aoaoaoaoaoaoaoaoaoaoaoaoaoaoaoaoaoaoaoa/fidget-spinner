//! The MCP server: tool schemas, dispatch, receipts and process telemetry.

use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    time::Instant,
};

use fidget_spinner_core::{Frontier, Op, Operation, Refusal};
use libmcp::{
    DetailLevel, FrameLimit, FrameReadOutcome, RenderMode, read_frame_blocking,
    write_frame_blocking,
};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use thiserror::Error;
use trebuchet::inspect::{Window, measures};

use crate::{
    history,
    inspect::{
        self, Cursor, HistoryArgs, Lens, Paging, ProjectScope, ReadArgs, SchemaArgs, SnapshotKey,
        StatusArgs, View,
    },
    store::{self, Address, Decision, Ledger, ProjectRoot, Standing, Store, StoreError},
};

const FRAME_LIMIT: usize = 16 << 20;
const PROTOCOL_VERSION: &str = "2025-11-25";
const VERSIONS: [&str; 4] = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
const INSTRUCTIONS: &str = "Fidget Spinner keeps an inquiry's record across sessions: open questions, attempts, judgments over evidence, supervision and optional measurement. It never runs experiments or verifies evidence. Each call takes optional project (default: the server's working directory; linked worktrees resolve to their main checkout) and frontier; frontier.patch requires frontier, and reads may omit it only when exactly one frontier is unconcluded. Work is trials and isets. A trial is an attempt: planned, amended by edit, and reported once against a control baseline. An iset (integration set) poses a question over member work and is judged once; its answer may land either way. Inputs order work; membership is not dependency. Every record resolves at most once and is voided at most once: a later answer is a new iset, and whoever voids sweeps what rests on the void. An annotation is a dated remark of at most 100 words on any record, resolved or void included; it changes nothing the record says. Schemas type data; baselines are comparison states, and elections choose the incumbent. A patch is atomic; warnings and findings never block. Read through frontier.status and record.read. Render selects representation, detail selects text completeness, and cursors freeze the ledger sequence. Removed records are redacted even in old history.";
pub const OPS_GRAMMAR: &str = r#"Apply ordered ops atomically; dry_run rehearses. Each op is an object with op and fields; work names a trial or iset, and work|baseline means exactly one of those keys.
frontier {objective}; conclude {lesson}; guard {head:seq}
regime {contract,metrics:[metric],reason?,bridge?:{slug,readings,note?}}
gauge {contract?,metrics?:[metric],retire?:[key],restore?:[key]}
define {schema,definition,rev?}; the definition grammar is schema.read without schema
iset {slug,schema?,data?,members?:[work],into?:[iset],after?:[work]}
plan {trial?,schema?,data?,control?:baseline,treatment?,into?:[iset],after?:[work]}
report {trial?,plan?:rev|"unknown",readings?,control_readings?,artifacts?:[text],data?}, plus {schema?,control?,treatment?,into?:[iset],after?:[work]} only when it creates its trial
edit {work,rev?,data?,set?:[{path,value}],delete?:[path],schema?,unbind?:true,control?,treatment?}
member {iset,add?:[work],drop?:[work]}; after {work,add?:[work],drop?:[work]}
judge {iset,answer,seen?:seq}
baseline {slug,artifacts?:[text],readings?,note?}; elect {baseline,rationale,adopts?:[iset]}
void {work|baseline,reason}; remove {work|baseline}; rename {work|baseline,to}
annotate {work|baseline,text}: one hyper-terse fact in at most 100 words; it changes nothing the record says, even resolved or void
assign {work,owner,instructions?}; release/start/stop {work}
metric={key,unit,better?:lower|higher,kpi?:bool,about?,refs?:{label:number}}; readings={key:number|'number unit'}; treatment={baseline:slug}|{identity:text}; path=[key,…]; schema=id|id@rev.
Units: ns/µs/us/ms/s/min/h (time); B/kB/MB/GB/TB/KiB/MiB/GiB/TiB (memory); 1/% (dimensionless); other lowercase words are count labels.
data and answer are JSON; {"$fs":"ref","kind":"trial|iset|baseline|metric","id":name|key@regime} references a record by current name. A trial without a name joins its first into iset and is named slug#n. report naming no trial creates a retrospective trial. A trial reports once and an iset is judged once; resolved and void work is frozen, though it may still join open isets. edit of work that predates the patch names its rev; edit of a trial amends its plan. judge with seen=S refuses if anything the judgment consumes changed after S. An unknown frontier requires first op frontier. A concluded frontier accepts only void, remove, rename and annotate. Archive is the owner's, in the navigator."#;
pub const SCHEMA_GRAMMAR: &str = r#"definition = {carrier:"iset",data:fields,answer:fields,title?,description?} | {carrier:"trial",plan:fields,report:fields,title?,description?}
fields = {name:field,…}
field = {type,presence:"required"|"optional",title?,description?,show?:"headline"|"badge"|"card"|"section"|"hidden"}
type = "bool"|"text"|"int"|"float"|{enum:[text,…]}|{list:type}|{map:type}|{record:fields}|{variant:{tag:fields,…}}|{ref:"trial"|"iset"|"baseline"|"metric"}
int admits integral numbers within ±(2^53−1); float admits any number; enum texts are nonempty and distinct; map keys are text; record and fields are closed; a variant value is {"tag","value"}; show is a display hint.
iset example: {"carrier":"iset","title":"Question","data":{"question":{"type":"text","presence":"required","show":"headline"}},"answer":{"verdict":{"type":{"enum":["holds","fails","unclear"]},"presence":"required","show":"badge"},"why":{"type":"text","presence":"required"}}}
trial example: {"carrier":"trial","title":"Attempt","plan":{"change":{"type":"text","presence":"required","show":"headline"}},"report":{"outcome":{"type":"text","presence":"required"},"evidence":{"type":{"list":{"ref":"trial"}},"presence":"optional"}}}"#;

#[derive(Debug, Error)]
pub enum Fault {
    #[error("{0}")]
    Arguments(String),
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    Store(#[from] StoreError<Refusal>),
    #[error("{0}")]
    Format(#[from] trebuchet::ledger::FormatError),
    #[error("{0}")]
    Refused(#[from] Refusal),
    #[error("{0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Navigator(#[from] trebuchet::board::ServeError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Tool {
    Patch,
    Status,
    Read,
    Schema,
    History,
    Health,
}

impl Tool {
    pub const ALL: [Self; 6] = [
        Self::Patch,
        Self::Status,
        Self::Read,
        Self::Schema,
        Self::History,
        Self::Health,
    ];
    pub const fn name(self) -> &'static str {
        match self {
            Self::Patch => "frontier.patch",
            Self::Status => "frontier.status",
            Self::Read => "record.read",
            Self::Schema => "schema.read",
            Self::History => "frontier.history",
            Self::Health => "system.health",
        }
    }
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|t| t.name() == name)
    }
    const fn description(self) -> &'static str {
        match self {
            Self::Patch => OPS_GRAMMAR,
            Self::Status => {
                "Summary, or paged isets, trials, baselines, regimes, schemas, findings and project frontiers. filter selects rows: under (an iset's members at any depth), lane, state (or void), owner, schema, text. scope=project with filter.text searches every frontier of the project, concluded included: frontier | record | field | excerpt. Regimes are newest first. Inapplicable selectors warn and are ignored. Counts and states derive from the whole frontier."
            }
            Self::Read => {
                "One trial or iset (work) or baseline, by section: head, data, plans, report, members, judgment, elections, isets, inputs, dependents, owner, annotations; a section the record lacks is ignored with a warning. Own text is exact; related text previews with full-read routes. Collections page independently."
            }
            Self::Schema => {
                "One schema id's definitions newest first, each with its revision, a line per variant or ref field showing a value to write, and the work bound to it; without schema, the definition grammar with an example of each carrier."
            }
            Self::History => {
                "Patch metadata newest first. seq with detail=full reads that patch's operations as written, paged. Operations naming removed records expose only the operation name and record kind."
            }
            Self::Health => {
                "Process and store health. Full adds bounded metadata-only process-local call, output, schema and projection counters; reset on restart."
            }
        }
    }
    pub fn describe(self) -> Value {
        let string = json!({"type":"string"});
        let limit =
            json!({"type":"integer","minimum":1,"description":"Rows per page, capped at 200."});
        let (mut properties, required) = match self {
            Self::Patch => (
                json!({"ops":{"type":"array","minItems":1,"items":{"type":"object","required":["op"]}},"dry_run":{"type":"boolean"}}),
                vec!["frontier", "ops"],
            ),
            Self::Status => (
                json!({
                "view":{"type":"string","enum":["summary","isets","trials","baselines","regimes","schemas","findings","frontiers"]},
                "scope":{"type":"string","enum":["project"]},
                "filter":{"type":"object","additionalProperties":false,"properties":{
                    "under":string,"lane":string,
                    "state":{"type":"array","items":{"type":"string","enum":["waiting","ready","running","tested","reported","settled","void"]}},
                    "owner":string,"schema":string,"text":string}},
                "limit":limit,"cursor":string}),
                vec![],
            ),
            Self::Read => (
                json!({"work":string,"baseline":string,"sections":{"type":"array","minItems":1,"uniqueItems":true,
                "items":{"type":"string","enum":["head","data","plans","report","members","judgment","elections","isets","inputs","dependents","owner","annotations"]}},
                "limit":limit,"cursor":string}),
                vec![],
            ),
            Self::Schema => (
                json!({"schema":string,"limit":limit,"cursor":string}),
                vec![],
            ),
            Self::History => (
                json!({"seq":{"type":"integer","minimum":1},"limit":limit,"cursor":string}),
                vec![],
            ),
            Self::Health => (json!({}), vec![]),
        };
        properties["project"] = string.clone();
        properties["frontier"] = string;
        let schema = libmcp::with_presentation_properties(
            json!({"type":"object","additionalProperties":false,
            "properties":properties,"required":required}),
        );
        json!({"name":self.name(),"description":self.description(),"inputSchema":schema,
            "annotations":{"readOnlyHint":self!=Self::Patch},
            "_meta":{"io.libmcp/effect":{"recovery":{"kind":if self==Self::Patch {"at_most_once"}else{"replay_safe"}},
                "state":{"kind":"stateless"}}}})
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddressArgs {
    pub project: Option<PathBuf>,
    pub frontier: Option<trebuchet::Slug>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchArgs {
    #[serde(deserialize_with = "parse_ops")]
    pub ops: Vec<Op>,
    #[serde(default)]
    pub dry_run: bool,
}

fn parse_ops<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Vec<Op>, D::Error> {
    use serde::de::Error as _;
    let ops = Vec::<Value>::deserialize(deserializer)?;
    if ops.is_empty() {
        return Err(D::Error::custom("a patch needs at least one operation"));
    }
    ops.into_iter()
        .enumerate()
        .map(|(index, value)| {
            let name = value["op"].as_str().unwrap_or("?").to_owned();
            Op::parse(value)
                .map_err(|error| D::Error::custom(format!("operation {index} ({name}): {error}")))
        })
        .collect()
}

#[derive(Default)]
struct Toll {
    calls: u64,
    errors: u64,
    output_bytes: u64,
    result_bytes: u64,
    micros: u128,
    rows: u64,
    incomplete_pages: u64,
    previews: u64,
}

pub struct Server {
    path: PathBuf,
    default_project: Result<ProjectRoot, String>,
    store: Option<Store>,
    tolls: BTreeMap<(Tool, String, String, String), Toll>,
}

impl Server {
    pub fn new(path: PathBuf, project: &Path) -> Self {
        Self {
            path,
            default_project: store::project_root(project)
                .map_err(|error| format!("working directory: {error}")),
            store: None,
            tolls: BTreeMap::new(),
        }
    }

    fn store(&mut self) -> Result<&mut Store, Fault> {
        if self.store.is_none() {
            self.store = Some(store::open(&self.path)?);
        }
        self.store
            .as_mut()
            .ok_or_else(|| Fault::Arguments("store unavailable after opening".into()))
    }

    fn project(&self, args: &AddressArgs) -> Result<ProjectRoot, Fault> {
        args.project.as_ref().map_or_else(
            || {
                self.default_project
                    .clone()
                    .map_err(|reason| Fault::Arguments(format!("{reason}; pass project")))
            },
            |p| Ok(store::project_root(p)?),
        )
    }

    fn address(&mut self, args: &AddressArgs, required: bool) -> Result<Address, Fault> {
        if required && args.frontier.is_none() {
            return Err(Fault::Arguments("frontier.patch requires frontier".into()));
        }
        let project = self.project(args)?;
        Ok(self
            .store()?
            .resolve(&project, args.frontier.as_ref(), |address, f| {
                (!f.archived()).then(|| Standing {
                    address,
                    live: f.conclusion().is_none(),
                    description: standing(f).into(),
                })
            })?)
    }

    fn context(&self, address: &Address) -> Value {
        let mut context = json!({"frontier":address.slug});
        if self.default_project.as_ref().ok() != Some(&address.project) {
            context["project"] = json!(address.project.as_path());
        }
        context
    }

    fn snapshot(
        &mut self,
        address: &Address,
        cursor: Option<&str>,
        tool: Tool,
        arguments: &Value,
    ) -> Result<Ledger<Frontier>, Fault> {
        let mut current = Ledger::default();
        self.store()?.refresh(address, &mut current)?;
        let Some(cursor) = cursor else {
            return Ok(current);
        };
        let key = SnapshotKey {
            address: json!({"project":address.project.as_path(),"frontier":address.slug}),
            seq: current.seq(),
            attachment: None,
        };
        let (query, collections) = continuation_query(tool, arguments)?;
        let cursor = Cursor::decode(
            cursor,
            |collection| key.digest(&query, collection),
            inspect::COLLECTIONS,
        )
        .map_err(Fault::Arguments)?;
        if !collections.contains(&cursor.collection.as_str()) {
            return Err(Fault::Arguments(
                "cursor belongs to another query; rerun without it".into(),
            ));
        }
        if cursor.seq > current.seq() {
            return Err(Fault::Arguments(
                "cursor sequence is in the future; rerun without it".into(),
            ));
        }
        if tool != Tool::History {
            self.check_retention(&current, cursor.seq)?;
        }
        self.store()?.snapshot(address, cursor.seq)?.ok_or_else(|| {
            Fault::Arguments("cursor belongs to another query; rerun without it".into())
        })
    }

    /// A cursor older than a removal would page through records that reads
    /// no longer show; refuse it.
    fn check_retention(&mut self, current: &Ledger<Frontier>, seq: i64) -> Result<(), Fault> {
        if seq < current.seq()
            && let Some(id) = current.id()
        {
            let count = self.store()?.history_count(id, current.seq())?;
            let patches = self.store()?.history_at(id, current.seq(), 0, count)?;
            if patches.iter().take_while(|p| p.seq > seq).any(|p| {
                p.ops
                    .iter()
                    .any(|op| matches!(op.operation(), Operation::Remove { .. }))
            }) {
                return Err(Fault::Arguments(
                    "records were removed after this snapshot; rerun the query without cursor"
                        .into(),
                ));
            }
        }
        Ok(())
    }

    pub fn invoke(&mut self, tool: Tool, arguments: Value) -> Result<Value, Fault> {
        let (render, output) = self.project_call(tool, arguments)?;
        Ok(rendered(output, render))
    }

    fn project_call(&mut self, tool: Tool, arguments: Value) -> Result<(RenderMode, Value), Fault> {
        let (render, detail, address, args) = split(arguments)?;
        let schema = tool.describe();
        let mut paths = Vec::new();
        argument_paths(&schema["inputSchema"], "", &mut paths);
        check_arguments(&args, &schema["inputSchema"], "", &paths, tool)?;
        let output = crate::report::project(self.dispatch(tool, &address, args, detail)?)?;
        Ok((render, output))
    }

    fn dispatch(
        &mut self,
        tool: Tool,
        args: &AddressArgs,
        arguments: Value,
        detail: DetailLevel,
    ) -> Result<Value, Fault> {
        if tool == Tool::Health {
            let _: Empty = parse(arguments)?;
            let mut result = self.health(detail);
            if let Some(frontier) = &args.frontier {
                result["frontier"] = json!(frontier);
            }
            if let Some(project) = &args.project {
                result["project"] = json!(store::project_root(project)?.as_path());
            }
            return Ok(result);
        }
        if tool == Tool::Status {
            let mut status: StatusArgs = parse(arguments.clone())?;
            let warnings = status.normalize();
            if status.view == View::Frontiers || status.scope == Some(ProjectScope::Project) {
                let mut result = self.project_status(args, &status, detail)?;
                if !warnings.is_empty() {
                    result["ignored"] = json!(warnings);
                }
                return Ok(result);
            }
        }
        let address = self.address(args, tool == Tool::Patch)?;
        let context = self.context(&address);
        if tool == Tool::Patch {
            let PatchArgs { ops, dry_run } = parse(arguments)?;
            fidget_spinner_core::bar_reserved(&ops)?;
            let mut ledger = Ledger::default();
            let receipt = self.store()?.commit(
                &address,
                &mut ledger,
                &ops,
                time::OffsetDateTime::now_utc(),
                dry_run,
            )?;
            let mut output = context.clone();
            match receipt.decision {
                Decision::Appended(seq) => output["seq"] = json!(seq),
                Decision::Rehearsed => output["rehearsed"] = json!(true),
            }
            let lens = Lens::new(&receipt.report.frontier, context, detail);
            if let (Some(to), Value::Object(from)) = (
                output.as_object_mut(),
                lens.receipt(&receipt.report, &receipt.warnings),
            ) {
                to.extend(from);
            }
            return Ok(output);
        }
        let cursor = arguments.get("cursor").and_then(Value::as_str);
        let ledger = self.snapshot(&address, cursor, tool, &arguments)?;
        if ledger.id().is_none() {
            let listings = self.store()?.listings_of(&address.project)?;
            return Err(Fault::Arguments(fidget_spinner_core::slug::suggestion(
                &format!("frontier {}", address.slug),
                &fidget_spinner_core::slug::nearest(
                    address.slug.as_str(),
                    listings.iter().map(|row| row.address.slug.as_str()),
                ),
            )));
        }
        let key = SnapshotKey {
            address: json!({"project":address.project.as_path(),"frontier":address.slug}),
            seq: ledger.seq(),
            attachment: None,
        };
        let paging = Paging::new(&key).with_collections(inspect::COLLECTIONS);
        let lens = Lens::new(ledger.state(), context.clone(), detail);
        let output = match tool {
            Tool::Status => lens
                .status(&parse(arguments)?, &paging)
                .map_err(Fault::Arguments)?,
            Tool::Read => lens
                .read(&parse::<ReadArgs>(arguments)?, &paging)
                .map_err(Fault::Arguments)?,
            Tool::Schema => lens
                .schema(&parse::<SchemaArgs>(arguments)?, &paging)
                .map_err(Fault::Arguments)?,
            Tool::History => {
                self.history(&address, &ledger, &parse(arguments)?, &paging, detail)?
            }
            Tool::Patch | Tool::Health => unreachable!(),
        };
        let mut result = context;
        result["seq"] = json!(ledger.seq());
        if let (Some(to), Some(from)) = (result.as_object_mut(), output.as_object()) {
            to.extend(from.clone());
        }
        Ok(result)
    }

    fn project_status(
        &mut self,
        args: &AddressArgs,
        status: &StatusArgs,
        detail: DetailLevel,
    ) -> Result<Value, Fault> {
        if status.view != View::Frontiers && status.filter.text.is_none() {
            return Err(Fault::Arguments(
                "scope=project requires filter.text, or view=frontiers".into(),
            ));
        }
        let project = self.project(args)?;
        let head = self.store()?.head()?;
        let high = status
            .window
            .cursor
            .as_deref()
            .map(|token| {
                let key = SnapshotKey {
                    address: json!({"project":project.as_path()}),
                    seq: head,
                    attachment: None,
                };
                let cursor = Cursor::decode(
                    token,
                    |collection| key.digest(&inspect::semantic(status), collection),
                    inspect::COLLECTIONS,
                )?;
                if cursor.collection != "project" {
                    return Err("cursor belongs to another query; rerun without it".into());
                }
                Ok(cursor)
            })
            .transpose()
            .map_err(Fault::Arguments)?
            .map_or(head, |c| c.seq);
        if high > head {
            return Err(Fault::Arguments("cursor sequence is in the future".into()));
        }
        let listings = self.store()?.listings_of(&project)?;
        let mut rows = Vec::new();
        let mut denominator = 0;
        for listing in listings {
            let mut latest = Ledger::default();
            self.store()?.refresh(&listing.address, &mut latest)?;
            let id = latest
                .id()
                .ok_or_else(|| Fault::Arguments("listing has no ledger".into()))?;
            let entry = self.store()?.history_at(id, high, 0, 1)?;
            let Some(seq) = entry.first().map(|e| e.seq) else {
                continue;
            };
            self.check_retention(&latest, high)?;
            let ledger = self
                .store()?
                .snapshot(&listing.address, seq)?
                .ok_or_else(|| Fault::Arguments("project snapshot disappeared".into()))?;
            let context = self.context(&listing.address);
            let lens = Lens::new(ledger.state(), context, detail);
            if status.view == View::Frontiers {
                denominator += 1;
                let objective = ledger.state().objective().unwrap_or("");
                if status
                    .filter
                    .text
                    .as_ref()
                    .is_some_and(|q| !inspect::contains(objective, q))
                {
                    continue;
                }
                rows.push(json!({"frontier":listing.address.slug,
                    "standing":standing(ledger.state()),
                    "counts":lens.counts(),
                    "objective":trebuchet::inspect::preview(objective,detail==DetailLevel::Full,160,||
                        trebuchet::inspect::Route{tool:"frontier.status".into(),arguments:json!({"project":project.as_path(),"frontier":listing.address.slug,"detail":"full"})})}));
            } else if status.view == View::Summary {
                denominator += lens.denominator();
                for row in lens.search(&status.filter) {
                    rows.push(json!({"frontier":listing.address.slug,"record":row["record"],"field":row["field"],"excerpt":row["excerpt"]}));
                }
            } else {
                let key = SnapshotKey {
                    address: json!({"project":project.as_path(),"frontier":listing.address.slug}),
                    seq,
                    attachment: None,
                };
                let mut local = StatusArgs {
                    scope: None,
                    window: Window {
                        limit: Some(200),
                        cursor: None,
                    },
                    ..status.clone()
                };
                loop {
                    let result = lens
                        .status(
                            &local,
                            &Paging::new(&key).with_collections(inspect::COLLECTIONS),
                        )
                        .map_err(Fault::Arguments)?;
                    if local.window.cursor.is_none() {
                        denominator += usize::try_from(result["scope_total"].as_u64().unwrap_or(0))
                            .map_err(|_| {
                                Fault::Arguments("row count exceeds address space".into())
                            })?;
                    }
                    if let Some(items) = result["items"].as_array() {
                        for item in items {
                            let mut item = item.clone();
                            item["frontier"] = json!(listing.address.slug);
                            rows.push(item);
                        }
                    }
                    let Some(cursor) = result["next_cursor"].as_str() else {
                        break;
                    };
                    local.window.cursor = Some(cursor.into());
                }
            }
        }
        let key = SnapshotKey {
            address: json!({"project":project.as_path()}),
            seq: high,
            attachment: None,
        };
        let page = Paging::new(&key)
            .with_collections(inspect::COLLECTIONS)
            .page(
                rows,
                &inspect::semantic(status),
                "project",
                &status.window,
                20,
                |r| r,
            )
            .map_err(Fault::Arguments)?;
        let mut result = json!({"seq":high,"scope_total":denominator,"items":page.items,"total":page.total,"next_cursor":page.next_cursor});
        if self.default_project.as_ref().ok() != Some(&project) {
            result["project"] = json!(project.as_path());
        }
        Ok(result)
    }

    fn history(
        &mut self,
        address: &Address,
        ledger: &Ledger<Frontier>,
        args: &HistoryArgs,
        paging: &Paging<'_>,
        detail: DetailLevel,
    ) -> Result<Value, Fault> {
        args.window.validate(true).map_err(Fault::Arguments)?;
        if args.seq.is_some_and(|s| s < 1) {
            return Err(Fault::Arguments("seq must be positive".into()));
        }
        if args.seq.is_some() && detail != DetailLevel::Full {
            return Err(Fault::Arguments("seq requires detail=full".into()));
        }
        let mut current = Ledger::default();
        self.store()?.refresh(address, &mut current)?;
        let id = current
            .id()
            .ok_or_else(|| Fault::Arguments("frontier has no history".into()))?;
        let total = self.store()?.history_count(id, current.seq())?;
        let all = self.store()?.history_at(id, current.seq(), 0, total)?;
        let redacted = history::redacted(&all)?;
        if let Some(seq) = args.seq {
            if seq > ledger.seq() {
                return Err(Fault::Arguments("seq is after this snapshot".into()));
            }
            let ops = redacted
                .get(&seq)
                .ok_or_else(|| Fault::Arguments(format!("no patch at sequence {seq}")))?;
            let page = paging
                .page(
                    ops.clone(),
                    &json!({"seq":seq}),
                    "ops",
                    &args.window,
                    20,
                    |op| op,
                )
                .map_err(Fault::Arguments)?;
            return Ok(
                json!({"patch":seq,"items":page.items,"total":page.total,"next_cursor":page.next_cursor}),
            );
        }
        let rows:Vec<_>=all.iter().filter(|e|e.seq<=ledger.seq()).map(|e|json!({"seq":e.seq,
            "at":inspect::stamp(e.at,detail==DetailLevel::Full),"operations":e.ops.len(),
            "targets":redacted.get(&e.seq).map(|ops|ops.iter().map(history::targets).collect::<Vec<_>>())})).collect();
        let page = paging
            .page(rows, &json!({}), "history", &args.window, 20, |r| r)
            .map_err(Fault::Arguments)?;
        Ok(json!({"items":page.items,"total":page.total,"next_cursor":page.next_cursor}))
    }

    fn health(&self, detail: DetailLevel) -> Value {
        let (available, head, error) = match store::read_only(&self.path) {
            Ok(Some(store)) => match store.head() {
                Ok(head) => (true, Some(head), None),
                Err(e) => (false, None, Some(e.to_string())),
            },
            Ok(None) => (false, None, None),
            Err(Fault::Store(StoreError::Version(version))) if version < store::FORMAT => (
                false,
                None,
                Some(format!(
                    "store format {version} migrates to {} when a writer first opens it",
                    store::FORMAT
                )),
            ),
            Err(e) => (false, None, Some(e.to_string())),
        };
        let mut result = json!({"default_project": match &self.default_project {
            Ok(project) => json!(project.as_path()),
            Err(reason) => json!({"error":reason}),
        }, "version":env!("CARGO_PKG_VERSION"),"process":"ready","store":self.path,
            "format":store::FORMAT,"available":available,"head":head,"error":error});
        if detail == DetailLevel::Full {
            result["telemetry"] = json!({"lifetime":"process-local; reset on restart","total":self.tolls.len(),"items":self.tolls.iter().map(|((tool,operation,render,detail),t)|json!({
                "tool":tool.name(),"operation":operation,"render":render,"detail":detail,"calls":t.calls,"errors":t.errors,
                "schema_utf8_bytes":tool.describe()["inputSchema"].to_string().len(),
                "output_utf8_bytes":t.output_bytes,"mcp_result_utf8_bytes":t.result_bytes,"micros":t.micros,
                "rows":t.rows,"incomplete_pages":t.incomplete_pages,"previews":t.previews})).collect::<Vec<_>>()});
        }
        result
    }

    pub fn handle(&mut self, message: &Value) -> Option<Value> {
        let id = message.get("id")?;
        let params = message.get("params").cloned().unwrap_or(Value::Null);
        let result = match message
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "initialize" => {
                json!({"protocolVersion":params["protocolVersion"].as_str().filter(|v|VERSIONS.contains(v)).unwrap_or(PROTOCOL_VERSION),
                "capabilities":{"tools":{"listChanged":false}},"serverInfo":{"name":"fidget-spinner","version":env!("CARGO_PKG_VERSION")},"instructions":INSTRUCTIONS})
            }
            "ping" => json!({}),
            "tools/list" => json!({"tools":Tool::ALL.map(Tool::describe)}),
            "tools/call" => {
                let name = params["name"].as_str().unwrap_or_default();
                let Some(tool) = Tool::parse(name) else {
                    return Some(rpc_error(
                        id,
                        -32602,
                        &fidget_spinner_core::slug::suggestion(
                            &format!("tool {name:?}"),
                            &fidget_spinner_core::slug::nearest(
                                name,
                                Tool::ALL.iter().map(|tool| tool.name()),
                            ),
                        ),
                    ));
                };
                let arguments = params
                    .get("arguments")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                self.call(tool, arguments)
            }
            method => {
                return Some(rpc_error(
                    id,
                    -32601,
                    &format!("method {method:?} not found"),
                ));
            }
        };
        Some(json!({"jsonrpc":"2.0","id":id,"result":result}))
    }

    fn call(&mut self, tool: Tool, arguments: Value) -> Value {
        let started = Instant::now();
        let render = label(&arguments, "render", &["porcelain", "json"], "porcelain");
        let detail = label(&arguments, "detail", &["concise", "full"], "concise");
        let present = |key: &str| arguments.get(key).is_some_and(|v| !v.is_null());
        let operation = match tool {
            Tool::Patch if arguments["dry_run"] == true => "rehearse".into(),
            Tool::Patch => "patch".into(),
            Tool::Status => label(
                &arguments,
                "view",
                &[
                    "summary",
                    "isets",
                    "trials",
                    "baselines",
                    "regimes",
                    "schemas",
                    "findings",
                    "frontiers",
                ],
                "summary",
            ),
            Tool::Read if present("baseline") => "baseline".into(),
            Tool::Read => "work".into(),
            Tool::Schema if present("schema") => "schema".into(),
            Tool::Schema => "grammar".into(),
            Tool::History if present("seq") => "ops".into(),
            Tool::History => "patches".into(),
            Tool::Health => "health".into(),
        };
        let (result, failed, projection_measures, bytes) = match self.project_call(tool, arguments)
        {
            Ok((render, projection)) => {
                let sizes = measures(&projection);
                let result = rendered(projection, render);
                let bytes = if render == RenderMode::Json {
                    result["structuredContent"].to_string().len()
                } else {
                    result["content"][0]["text"].as_str().map_or(0, str::len)
                };
                (result, false, sizes, bytes)
            }
            Err(error) => {
                let text = error.to_string();
                let bytes = text.len();
                (
                    json!({"content":[{"type":"text","text":text}],"isError":true}),
                    true,
                    (0, 0, 0),
                    bytes,
                )
            }
        };
        let toll = self
            .tolls
            .entry((tool, operation, render, detail))
            .or_default();
        toll.calls += 1;
        toll.errors += u64::from(failed);
        toll.output_bytes += bytes as u64;
        toll.result_bytes += result.to_string().len() as u64;
        toll.micros += started.elapsed().as_micros();
        toll.rows += projection_measures.0;
        toll.incomplete_pages += projection_measures.1;
        toll.previews += projection_measures.2;
        result
    }

    pub fn serve(mut self) -> io::Result<()> {
        let limit =
            FrameLimit::try_new(FRAME_LIMIT).map_err(|e| io::Error::other(e.to_string()))?;
        let mut input = io::stdin().lock();
        let mut output = io::stdout().lock();
        loop {
            let frame = match read_frame_blocking(&mut input, limit)? {
                FrameReadOutcome::Frame(frame) => frame,
                FrameReadOutcome::EndOfStream => return Ok(()),
            };
            let reply = match serde_json::from_slice::<Value>(&frame) {
                Ok(message) => self.handle(&message),
                Err(e) => Some(rpc_error(&Value::Null, -32700, &e.to_string())),
            };
            if let Some(reply) = reply {
                let mut frame = serde_json::to_vec(&reply)?;
                if frame.len() > FRAME_LIMIT {
                    frame = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":reply["id"],
                        "result":{"content":[{"type":"text","text":"response exceeds frame limit; select fewer sections or a smaller limit"}],"isError":true}}))?;
                }
                write_frame_blocking(&mut output, &frame, limit)?;
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

fn rendered(output: Value, render: RenderMode) -> Value {
    match render {
        RenderMode::Porcelain => {
            json!({"content":[{"type":"text","text":crate::report::render(&output)}],"isError":false})
        }
        RenderMode::Json => {
            let mut result = json!({"content":[],"isError":false});
            result["structuredContent"] = output;
            result
        }
    }
}
fn split(arguments: Value) -> Result<(RenderMode, DetailLevel, AddressArgs, Value), Fault> {
    let Value::Object(mut fields) = arguments else {
        return Err(Fault::Arguments("arguments must be an object".into()));
    };
    let render = fields
        .remove("render")
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();
    let detail = fields
        .remove("detail")
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();
    let address =
        parse(json!({"project":fields.remove("project"),"frontier":fields.remove("frontier")}))?;
    Ok((render, detail, address, Value::Object(fields)))
}
fn parse<T: DeserializeOwned>(arguments: Value) -> Result<T, Fault> {
    serde_json::from_value(arguments).map_err(|e| Fault::Arguments(e.to_string()))
}
pub const fn standing(f: &Frontier) -> &'static str {
    if f.archived() {
        "archived"
    } else if f.conclusion().is_some() {
        "concluded"
    } else {
        "active"
    }
}
fn label(arguments: &Value, key: &str, valid: &[&str], default: &str) -> String {
    arguments[key]
        .as_str()
        .map_or(default, |s| if valid.contains(&s) { s } else { "invalid" })
        .into()
}
fn argument_properties(schema: &Value) -> Option<&serde_json::Map<String, Value>> {
    schema["properties"].as_object().or_else(|| {
        schema["oneOf"]
            .as_array()?
            .iter()
            .find_map(|variant| variant["properties"].as_object())
    })
}
fn argument_paths(schema: &Value, prefix: &str, paths: &mut Vec<String>) {
    if let Some(properties) = argument_properties(schema) {
        for (key, value) in properties {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            paths.push(path.clone());
            argument_paths(value, &path, paths);
        }
    }
}
fn check_arguments(
    arguments: &Value,
    schema: &Value,
    prefix: &str,
    paths: &[String],
    tool: Tool,
) -> Result<(), Fault> {
    let Some(properties) = argument_properties(schema) else {
        return Ok(());
    };
    for (key, value) in arguments
        .as_object()
        .into_iter()
        .flat_map(|fields| fields.iter())
    {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        if let Some(schema) = properties.get(key) {
            check_arguments(value, schema, &path, paths, tool)?;
        } else {
            let exact: Vec<_> = paths
                .iter()
                .filter(|candidate| candidate.rsplit('.').next() == Some(key.as_str()))
                .take(3)
                .cloned()
                .collect();
            let nearby = if exact.is_empty() {
                fidget_spinner_core::slug::nearest(&path, paths.iter().map(String::as_str))
            } else {
                exact
            };
            return Err(Fault::Arguments(format!(
                "{} for {}",
                fidget_spinner_core::slug::suggestion(&format!("argument {path:?}"), &nearby),
                tool.name()
            )));
        }
    }
    Ok(())
}
fn continuation_query(tool: Tool, arguments: &Value) -> Result<(Value, Vec<&'static str>), Fault> {
    match tool {
        Tool::Status => {
            let mut args: StatusArgs = parse(arguments.clone())?;
            args.normalize();
            Ok((inspect::semantic(&args), vec!["status"]))
        }
        Tool::Read => {
            let args: ReadArgs = parse(arguments.clone())?;
            let collections = args.sections.map_or_else(
                || inspect::READ_COLLECTIONS.to_vec(),
                |sections| {
                    sections
                        .into_iter()
                        .map(trebuchet::inspect::Part::name)
                        .collect()
                },
            );
            Ok((
                json!({"work":args.work,"baseline":args.baseline}),
                collections,
            ))
        }
        Tool::Schema => {
            let args: SchemaArgs = parse(arguments.clone())?;
            Ok((json!({"schema":args.schema}), vec!["bound"]))
        }
        Tool::History => {
            let args: HistoryArgs = parse(arguments.clone())?;
            Ok(args.seq.map_or_else(
                || (json!({}), vec!["history"]),
                |seq| (json!({"seq":seq}), vec!["ops"]),
            ))
        }
        Tool::Patch | Tool::Health => unreachable!(),
    }
}
fn rpc_error(id: &Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}
