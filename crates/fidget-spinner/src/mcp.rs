use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    time::Instant,
};

use fidget_spinner_core::{Conclusion, Created, Frontier, Op, Refusal};
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
        self, Cursor, HistoryArgs, Lens, Paging, ReadArgs, ReviewArgs, Scope, SnapshotKey,
        StatusArgs, View,
    },
    store::{self, Address, Decision, Ledger, ProjectRoot, Standing, Store, StoreError},
};

const FRAME_LIMIT: usize = 16 << 20;
const PROTOCOL_VERSION: &str = "2025-11-25";
const VERSIONS: [&str; 4] = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
const INSTRUCTIONS: &str = "Fidget Spinner records assertions about an inquiry; it never runs experiments or verifies evidence. Each call addresses optional project (default: server working directory) and frontier. Patch requires frontier. Reads may omit frontier only when exactly one frontier is active; otherwise the refusal lists unarchived candidates. Project paths resolve through linked Git worktrees to their main checkout. Frontier is an objective under a gauge; hypotheses are claims; trials are measurements; baselines are comparison states; reviews teach lessons over members. A patch is atomic. Findings never block operations. Read slowly through status and individual packets. Render selects representation, detail selects text completeness, and cursors freeze the ledger sequence. Removed records are redacted even in old history.";
pub const OPS_GRAMMAR: &str = "Apply ordered ops atomically; dry_run rehearses. Each object has op and fields:
frontier {objective}; conclude {lesson}; archive {}; regime {contract,metrics:[metric],reason?,bridge?:{slug,readings,note?}}
gauge {contract?,metrics?:[metric],retire?:[key],restore?:[key]}
baseline {slug,artifacts?:[text],readings?:{key:value},adopts?:[slug],docket?:[slug],note?}
docket {baseline,add?:[slug],drop?:[slug]}
hypothesis {slug,title?,mechanism?,claim?,after?:[slug],reviews?:[slug]}
edge {from,to,carries?}; unedge {from,to}
review {slug,title?,charter?,members?:[slug],add?:[slug],drop?:[slug]}
trial {hypothesis,note,readings?:{key:value},against?:baseline,artifacts?:[text]}
accept/reject {hypothesis|baseline,rationale}; shelve {hypothesis,rationale}; reopen {hypothesis|review}
void {trial|baseline,reason}; unvoid {trial|baseline}; remove {hypothesis|review|baseline|trial}
lesson {review,text}; waive {finding,note}; assign {hypothesis|review,owner,instructions?}
release/start/stop {hypothesis|review}; rename {hypothesis|review|baseline,to}
metric={key,unit,better?:lower|higher,kpi?:bool,about?,refs?:{label:number}}; value=number|'number unit'.
Units: ns/µs/us/ms/s/min/h (time); B/kB/MB/GB/TB/KiB/MiB/GiB/TiB (memory); 1/%; other lowercase words are count labels.\nSeparate targets with | require exactly one key. Trials use slug#n. Upserts preserve omitted/null fields; new hypotheses require title; new reviews require title,charter,members. Gauge metrics update by key. Baseline readings merge; artifacts/adopts/docket and review members replace. after/reviews and add/drop edit. Unknown frontier requires first op frontier. archive hides a concluded frontier from the navigator index and omitted-frontier candidates until it reopens.";

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
    Hypothesis,
    Review,
    History,
    Health,
}

impl Tool {
    pub const ALL: [Self; 6] = [
        Self::Patch,
        Self::Status,
        Self::Hypothesis,
        Self::Review,
        Self::History,
        Self::Health,
    ];
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Patch => "frontier.patch",
            Self::Status => "frontier.status",
            Self::Hypothesis => "hypothesis.read",
            Self::Review => "review.read",
            Self::History => "frontier.history",
            Self::Health => "system.health",
        }
    }
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|t| t.name() == name)
    }
    fn description(self) -> &'static str {
        match self {
            Self::Patch => OPS_GRAMMAR,
            Self::Status => {
                "Summary or paged hypotheses, reviews, trials, baselines, regimes, findings and project frontiers. Scope selects the denominator; filter selects rows. scope=project with filter.text searches all project frontiers, concluded included: frontier | record | field | excerpt. Read slug#n via hypothesis.read trials; other slugs via hypothesis.read, review.read or the baselines view. Regimes are newest first. Inapplicable filters/scopes warn and are ignored. Counts and readiness remain globally derived."
            }
            Self::Hypothesis => {
                "One hypothesis packet: claim, trials, disposition, inputs, outputs, reviews, docket, owner. Own text is exact; related text and trial notes preview with full-read routes. Collections page independently."
            }
            Self::Review => {
                "One review packet: charter, members, lesson, owner. Own text is exact; member rationales preview with full-read routes. Members are paged."
            }
            Self::History => {
                "Patch metadata newest first. seq with detail=full reads operations, paged. Operations on removed record lifetimes expose only operation name and record kind."
            }
            Self::Health => {
                "Process and store health. Full adds bounded metadata-only process-local call, output, schema and projection counters; reset on restart."
            }
        }
    }
    #[must_use]
    pub fn describe(self) -> Value {
        let string = json!({"type":"string"});
        let limit =
            json!({"type":"integer","minimum":1,"description":"Rows per page, capped at 200."});
        let (mut properties, required) = match self {
            Self::Patch => (
                json!({"ops":{"type":"array","items":{"type":"object","required":["op"]}},"dry_run":{"type":"boolean"}}),
                vec!["frontier", "ops"],
            ),
            Self::Status => (
                json!({
                "view":{"type":"string","enum":["summary","hypotheses","reviews","trials","baselines","regimes","findings","frontiers"]},
                "scope":{"oneOf":[{"type":"string","enum":["project"]},{"type":"object","additionalProperties":false,"properties":{"under":string,"review":string,"docket":string}}]},
                "filter":{"type":"object","additionalProperties":false,"properties":{
                    "state":{"type":"array","items":{"type":"string","enum":["waiting","ready","running","tested","settled","completed"]}},
                    "disposition":{"type":"array","items":{"type":"string","enum":["accepted","rejected","shelved","candidate"]}},
                    "owner":string,"lane":string,"text":string}},
                "limit":limit,"cursor":string}),
                vec![],
            ),
            Self::Hypothesis => (
                json!({"hypothesis":string,"sections":{"type":"array","minItems":1,"uniqueItems":true,
                "items":{"type":"string","enum":["claim","trials","disposition","inputs","outputs","reviews","docket","owner"]}},"limit":limit,"cursor":string}),
                vec!["hypothesis"],
            ),
            Self::Review => (
                json!({"review":string,"sections":{"type":"array","minItems":1,"uniqueItems":true,
                "items":{"type":"string","enum":["charter","members","lesson","owner"]}},"limit":limit,"cursor":string}),
                vec!["review"],
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
    Vec::<Value>::deserialize(deserializer)?
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            let name = value["op"].as_str().unwrap_or("?").to_owned();
            serde_json::from_value(value)
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
    #[must_use]
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
                match f.conclusion() {
                    Some(Conclusion { archived: true, .. }) => None,
                    conclusion => Some(Standing {
                        address,
                        live: conclusion.is_none(),
                        description: standing(f).into(),
                    }),
                }
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
        if let Some(cursor) = cursor {
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
        } else {
            Ok(current)
        }
    }

    fn check_retention(&mut self, current: &Ledger<Frontier>, seq: i64) -> Result<(), Fault> {
        if seq < current.seq()
            && let Some(id) = current.id()
        {
            let count = self.store()?.history_count(id, current.seq())?;
            let patches = self.store()?.history_at(id, current.seq(), 0, count)?;
            if patches
                .iter()
                .take_while(|p| p.seq > seq)
                .any(|p| p.ops.iter().any(|op| matches!(op, Op::Remove { .. })))
            {
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
            let mut result = self.health(detail)?;
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
            if status.view == View::Frontiers || matches!(status.scope, Some(Scope::Project(_))) {
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
            let mut ledger = Ledger::default();
            let receipt = self.store()?.commit(
                &address,
                &mut ledger,
                &ops,
                time::OffsetDateTime::now_utc(),
                dry_run,
            )?;
            let mut output = context;
            output["seq"] = match receipt.decision {
                Decision::Appended(seq) => json!(seq),
                Decision::Rehearsed => Value::Null,
            };
            if dry_run {
                output["rehearsed"] = json!(true);
            }
            output["created"] = json!(receipt.report.created.into_iter().map(|created| match created {
                Created::Epoch { baseline, at } => json!({"epoch":{"baseline":baseline,"since":inspect::stamp(at,detail==DetailLevel::Full)}}),
                created => json!(created),
            }).collect::<Vec<_>>());
            let warnings: Vec<_> = receipt
                .warnings
                .into_iter()
                .flat_map(|group| {
                    group.warnings.into_iter().map(move |warning| {
                        let mut row = json!(warning);
                        row["index"] = json!(group.index);
                        row
                    })
                })
                .collect();
            if !warnings.is_empty() {
                output["warnings"] = json!({"total":warnings.len(),"items":warnings});
            }
            for (key, findings) in [
                ("raised", receipt.report.raised),
                ("cleared", receipt.report.cleared),
            ] {
                if !findings.is_empty() {
                    output[key] = json!({"items":findings.iter().map(|(identity,waiver)|json!({"identity":identity,"waiver":waiver})).collect::<Vec<_>>(),"total":findings.len()});
                }
            }
            let mut changes = serde_json::Map::new();
            for state in receipt
                .report
                .before
                .keys()
                .chain(receipt.report.after.keys())
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
            {
                let before = receipt.report.before.get(&state).copied().unwrap_or(0);
                let after = receipt.report.after.get(&state).copied().unwrap_or(0);
                if before != after {
                    let key = serde_json::from_value::<String>(serde_json::to_value(state)?)?;
                    let _ = changes.insert(key, json!({"before":before,"after":after}));
                }
            }
            if !changes.is_empty() {
                output["changes"] = Value::Object(changes);
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
            Tool::Hypothesis => lens
                .hypothesis(&parse::<ReadArgs>(arguments)?, &paging)
                .map_err(Fault::Arguments)?,
            Tool::Review => lens
                .review(&parse::<ReviewArgs>(arguments)?, &paging)
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
                    .is_some_and(|q| !objective.to_lowercase().contains(&q.to_lowercase()))
                {
                    continue;
                }
                rows.push(json!({"frontier":listing.address.slug,
                    "standing":standing(ledger.state()),
                    "counts":lens.assay.counts,
                    "objective":trebuchet::inspect::preview(objective,detail==DetailLevel::Full,160,||
                        trebuchet::inspect::Route{tool:"frontier.status".into(),arguments:json!({"project":project.as_path(),"frontier":listing.address.slug,"detail":"full"})})}));
            } else if status.view == View::Summary {
                denominator += ledger
                    .state()
                    .hypotheses()
                    .values()
                    .map(|hypothesis| 1 + hypothesis.trials.len())
                    .sum::<usize>()
                    + ledger.state().reviews().len()
                    + ledger.state().baselines().len();
                for row in lens.search(&status.filter) {
                    rows.push(json!({"frontier":listing.address.slug,"record":row["record"],"field":row["field"],"excerpt":row["excerpt"]}));
                }
            } else {
                let local = StatusArgs {
                    scope: None,
                    window: Window {
                        limit: Some(200),
                        cursor: None,
                    },
                    ..status.clone()
                };
                let key = SnapshotKey {
                    address: json!({"project":project.as_path(),"frontier":listing.address.slug}),
                    seq,
                    attachment: None,
                };
                let mut local = local;
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

    fn health(&mut self, detail: DetailLevel) -> Result<Value, Fault> {
        let readable = Store::open_read_only(&self.path, store::format()?);
        let (available, head, error) = match readable {
            Ok(Some(store)) => match store.head() {
                Ok(head) => (true, Some(head), None),
                Err(e) => (false, None, Some(e.to_string())),
            },
            Ok(None) => (false, None, None),
            Err(e) => (false, None, Some(e.to_string())),
        };
        let mut result = json!({"default_project": match &self.default_project {
            Ok(project) => json!(project.as_path()),
            Err(reason) => json!({"error":reason}),
        }, "version":env!("CARGO_PKG_VERSION"),"process":"ready","store":self.path,
            "format":21,"available":available,"head":head,"error":error});
        if detail == DetailLevel::Full {
            result["telemetry"] = json!({"lifetime":"process-local; reset on restart","total":self.tolls.len(),"items":self.tolls.iter().map(|((tool,operation,render,detail),t)|json!({
                "tool":tool.name(),"operation":operation,"render":render,"detail":detail,"calls":t.calls,"errors":t.errors,
                "schema_utf8_bytes":tool.describe()["inputSchema"].to_string().len(),
                "output_utf8_bytes":t.output_bytes,"mcp_result_utf8_bytes":t.result_bytes,"micros":t.micros,
                "rows":t.rows,"incomplete_pages":t.incomplete_pages,"previews":t.previews})).collect::<Vec<_>>()});
        }
        Ok(result)
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
        let default_operation = match tool {
            Tool::Patch => {
                if arguments["dry_run"] == true {
                    "rehearse"
                } else {
                    "patch"
                }
            }
            Tool::Status => "summary",
            Tool::Hypothesis | Tool::Review => "packet",
            Tool::History => {
                if arguments.get("seq").is_some_and(|s| !s.is_null()) {
                    "ops"
                } else {
                    "patches"
                }
            }
            Tool::Health => "health",
        };
        let operation = label(
            &arguments,
            "view",
            &[
                "summary",
                "hypotheses",
                "reviews",
                "trials",
                "baselines",
                "regimes",
                "findings",
                "frontiers",
            ],
            default_operation,
        );
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
fn standing(f: &Frontier) -> &'static str {
    match f.conclusion() {
        None => "active",
        Some(Conclusion { archived: true, .. }) => "archived",
        Some(_) => "concluded",
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
            let _ = args.normalize();
            Ok((inspect::semantic(&args), vec!["status"]))
        }
        Tool::Hypothesis => {
            let args: ReadArgs = parse(arguments.clone())?;
            let collections = if let Some(sections) = args.sections {
                sections
                    .into_iter()
                    .map(|section| trebuchet::inspect::Part::name(section))
                    .collect()
            } else {
                vec!["trials", "inputs", "outputs", "reviews"]
            };
            Ok((json!({"hypothesis":args.hypothesis}), collections))
        }
        Tool::Review => {
            let args: ReviewArgs = parse(arguments.clone())?;
            let collections = if let Some(sections) = args.sections {
                sections
                    .into_iter()
                    .map(|section| trebuchet::inspect::Part::name(section))
                    .collect()
            } else {
                vec!["members"]
            };
            Ok((json!({"review":args.review}), collections))
        }
        Tool::History => {
            let args: HistoryArgs = parse(arguments.clone())?;
            Ok(if let Some(seq) = args.seq {
                (json!({"seq":seq}), vec!["ops"])
            } else {
                (json!({}), vec!["history"])
            })
        }
        Tool::Patch | Tool::Health => unreachable!(),
    }
}
fn rpc_error(id: &Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}
