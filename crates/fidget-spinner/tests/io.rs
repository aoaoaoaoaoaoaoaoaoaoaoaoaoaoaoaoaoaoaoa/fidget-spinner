#![expect(
    unused_crate_dependencies,
    reason = "Integration tests exercise selected public boundary dependencies."
)]

use std::{
    error::Error,
    io::{self, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command, Output, Stdio},
};

use libmcp::{FrameLimit, FrameReadOutcome, read_frame_blocking, write_frame_blocking};
use libmcp_testkit::TestCell;
use serde_json::{Value, json};
use trebuchet::ledger::rusqlite::{Connection, params};

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;

struct Reaper(Child);
impl Drop for Reaper {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

struct Session {
    _process: Reaper,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    id: u64,
    _cell: TestCell,
}

impl Session {
    fn new() -> Result<Self> {
        let cell = TestCell::new("spinner-session")?;
        let process = Command::new(env!("CARGO_BIN_EXE_fidget-spinner"))
            .args(["mcp", "serve"])
            .env("XDG_STATE_HOME", cell.path())
            .current_dir(cell.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let mut process = Reaper(process);
        let input = process
            .0
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("missing child stdin"))?;
        let output = process
            .0
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("missing child stdout"))?;
        let mut session = Self {
            _process: process,
            input,
            output: BufReader::new(output),
            id: 0,
            _cell: cell,
        };
        let init = session.rpc(
            "initialize",
            json!({"protocolVersion":"2025-11-25","capabilities":{},
            "clientInfo":{"name":"spinner-acceptance","version":"1"}}),
        )?;
        assert_eq!(init["serverInfo"]["name"], "fidget-spinner");
        let notification = json!({"jsonrpc":"2.0","method":"notifications/initialized"});
        write_frame_blocking(
            &mut session.input,
            &serde_json::to_vec(&notification)?,
            FrameLimit::try_new(16 << 20)?,
        )?;
        Ok(session)
    }

    fn rpc(&mut self, method: &str, params: Value) -> Result<Value> {
        self.id += 1;
        let mut request = json!({"jsonrpc":"2.0","id":self.id,"method":method});
        request["params"] = params;
        write_frame_blocking(
            &mut self.input,
            &serde_json::to_vec(&request)?,
            FrameLimit::try_new(16 << 20)?,
        )?;
        let FrameReadOutcome::Frame(frame) =
            read_frame_blocking(&mut self.output, FrameLimit::try_new(16 << 20)?)?
        else {
            return Err(io::Error::other("server closed stdout").into());
        };
        let response: Value = serde_json::from_slice(&frame)?;
        assert_eq!(response["id"], self.id);
        assert!(response.get("error").is_none(), "{response}");
        Ok(response["result"].clone())
    }

    fn call(&mut self, name: &str, arguments: Value) -> Result<Value> {
        let mut params = json!({"name":name});
        params["arguments"] = arguments;
        self.rpc("tools/call", params)
    }

    fn json(&mut self, name: &str, mut arguments: Value) -> Result<Value> {
        arguments["render"] = json!("json");
        let result = self.call(name, arguments)?;
        assert_eq!(result["isError"], false, "{result}");
        Ok(result["structuredContent"].clone())
    }
}

fn cli(cell: &TestCell, args: &[&str]) -> io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_fidget-spinner"))
        .args(args)
        .env("XDG_STATE_HOME", cell.path())
        .current_dir(cell.path())
        .output()
}

#[test]
fn wire_rendering_is_exclusive() -> Result {
    let mut session = Session::new()?;
    let porcelain = session.call("system.health", json!({}))?;
    assert!(porcelain.get("structuredContent").is_none());
    assert_eq!(porcelain["content"][0]["type"], "text");
    let json = session.call("system.health", json!({"render":"json"}))?;
    assert!(json["structuredContent"].is_object());
    assert_eq!(json["content"], json!([]));
    Ok(())
}

// Nested facts appear across packets, summaries and list views. Porcelain must
// keep them without leaking structural JSON into table cells; only data and
// answers render as compact JSON.
#[test]
fn porcelain_cells_are_not_json() -> Result {
    let mut session = Session::new()?;
    session.json("frontier.patch",json!({"frontier":"fixture","ops":[
        {"op":"frontier","objective":"fixture"},
        {"op":"regime","contract":"fixture protocol","metrics":[{"key":"elapsed","unit":"s","better":"lower","kpi":true,"refs":{"target":1}}]},
        {"op":"baseline","slug":"initial","artifacts":["commit","evidence"],"readings":{"elapsed":10}},
        {"op":"elect","baseline":"initial","rationale":"starting point"},
        {"op":"define","schema":"question","definition":{"carrier":"iset",
            "data":{"question":{"type":"text","presence":"required","show":"headline"}},
            "answer":{"verdict":{"type":{"enum":["holds","fails"]},"presence":"required","show":"badge"},"why":{"type":"text","presence":"optional"}}}},
        {"op":"iset", "title": "T","slug":"cache","schema":"question","data":{"question":"Does a cache help?"}},
        {"op":"plan", "synopsis": "S","into":["cache"],"data":{"change":"memoize"}},
        {"op":"report","trial":"cache#1","readings":{"elapsed":8},"artifacts":["trial evidence","{\"nested\":true}"],
            "data":{"note":"fixture result","see":{"$fs":"ref","kind":"baseline","id":"initial"}}},
        {"op":"judge","iset":"cache","answer":{"verdict":"holds","why":"fixture rationale"}},
        {"op":"iset", "title": "T","slug":"ready","data":{"question":"Next"},"after":["cache"]},
        {"op":"iset", "title": "T","slug":"program","data":{"charter":"fixture charter"},"members":["cache","ready"]},
        {"op":"regime","contract":"new fixture protocol","reason":"new fixture","metrics":[{"key":"elapsed","unit":"s","better":"lower","kpi":true}]}
    ]}))?;
    for detail in ["concise", "full"] {
        let mut calls = Vec::new();
        for view in [
            "summary",
            "isets",
            "trials",
            "baselines",
            "regimes",
            "schemas",
            "findings",
            "frontiers",
        ] {
            calls.push(("frontier.status", json!({"view":view,"detail":detail})));
        }
        calls.extend([
            (
                "frontier.status",
                json!({"scope":"project","filter":{"text":"fixture"},"detail":detail}),
            ),
            ("record.read", json!({"work":"cache","detail":detail})),
            ("record.read", json!({"work":"cache#1","detail":detail})),
            ("record.read", json!({"work":"program","detail":detail})),
            ("record.read", json!({"baseline":"initial","detail":detail})),
            ("schema.read", json!({"schema":"question","detail":detail})),
            ("schema.read", json!({"detail":detail})),
            ("frontier.history", json!({"detail":detail})),
            ("system.health", json!({"detail":detail})),
        ]);
        calls.push(("frontier.history", json!({"seq":1,"detail":"full"})));
        for (name, arguments) in calls {
            let table = name == "frontier.status"
                && arguments.get("view").is_some_and(|view| view != "summary");
            let result = session.call(name, arguments)?;
            assert_eq!(result["isError"], false, "{name}: {result}");
            assert!(result.get("structuredContent").is_none());
            let text = result["content"][0]["text"]
                .as_str()
                .ok_or("missing text")?;
            if table {
                assert!(
                    text.lines().any(|line| line.contains(" | ")),
                    "{name}: {text}"
                );
            }
            let mut header: Option<Vec<&str>> = None;
            for line in text.lines() {
                if !line.contains(" | ") {
                    header = None;
                    continue;
                }
                let Some(columns) = &header else {
                    header = Some(line.split(" | ").collect());
                    continue;
                };
                for (column, cell) in columns.iter().zip(line.split(" | ")) {
                    if matches!(*column, "data" | "answer" | "definition") {
                        continue;
                    }
                    if let Ok(value) = serde_json::from_str::<Value>(cell.trim()) {
                        assert!(!value.is_array() && !value.is_object(), "{name}: {line}");
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn mcp_inquiry_session() -> Result {
    let mut session = Session::new()?;
    let tools = session.rpc("tools/list", json!({}))?;
    assert_eq!(tools["tools"].as_array().map(Vec::len), Some(6));
    let receipt=session.json("frontier.patch",json!({"frontier":"latency","ops":[
        {"op":"frontier","objective":"Reduce latency without changing behavior"},
        {"op":"regime","contract":"one worker; fixed cases","metrics":[{"key":"elapsed","unit":"ms","better":"lower","kpi":true}]},
        {"op":"baseline","slug":"initial","readings":{"elapsed":10}},
        {"op":"elect","baseline":"initial","rationale":"Measured starting point"}
    ]}))?;
    assert_eq!(receipt["seq"], 1);
    let patched=session.json("frontier.patch",json!({"frontier":"latency","ops":[
        {"op":"iset", "title": "T","slug":"parse.cache","data":{"title":"Cache parsing"}},
        {"op":"plan", "synopsis": "S","into":["parse.cache"]},
        {"op":"report","trial":"parse.cache#1","readings":{"elapsed":8},"data":{"note":"Identical outputs; 20% faster"}},
        {"op":"iset", "title": "T","slug":"discarded","data":{"title":"Secret removed title"}},
        {"op":"rename","work":"discarded","to":"renamed"},
        {"op":"remove","work":"renamed"},
        {"op":"iset", "title": "T","slug":"renamed","data":{"title":"A distinct record"}}
    ]}))?;
    assert_eq!(patched["changes"]["iset tested"]["after"], 1);
    assert_eq!(patched["created"][1]["trial"], "parse.cache#1");
    let refused = session.call(
        "frontier.patch",
        json!({"frontier":"latency","ops":[{"op":"judge","iset":"parse.cache","answer":{},"seen":1}]}),
    )?;
    assert_eq!(refused["isError"], true);
    let status = session.json("frontier.status", json!({}))?;
    assert_eq!(status["frontier"], "latency");
    assert_eq!(status["counts"]["isets"]["tested"], 1);
    assert_eq!(status["incumbent_baseline"]["slug"], "initial");
    let packet = session.json(
        "record.read",
        json!({"work":"parse.cache#1","sections":["report"]}),
    )?;
    assert_eq!(packet["report"]["comparisons"]["elapsed"]["ratio"], 0.8);
    let history = session.json("frontier.history", json!({"seq":2,"detail":"full"}))?;
    assert!(!history.to_string().contains("Secret removed title"));
    assert_eq!(history["items"][3], json!({"op":"iset","kind":"iset"}));
    assert_eq!(history["items"][6]["data"]["title"], "A distinct record");
    let page = session.json("frontier.status", json!({"view":"isets","limit":1}))?;
    let cursor = page["next_cursor"]
        .as_str()
        .ok_or("missing status cursor")?;
    let history_page = session.json("frontier.history", json!({"limit":1}))?;
    let history_cursor = history_page["next_cursor"]
        .as_str()
        .ok_or("missing history cursor")?;
    session.json(
        "frontier.patch",
        json!({"frontier":"latency","ops":[{"op":"remove","work":"renamed"}]}),
    )?;
    let stale = session.call(
        "frontier.status",
        json!({"view":"isets","limit":1,"cursor":cursor}),
    )?;
    assert_eq!(stale["isError"], true);
    assert!(
        stale["content"][0]["text"]
            .as_str()
            .is_some_and(|s| s.contains("rerun"))
    );
    let old_history = session.json(
        "frontier.history",
        json!({"limit":1,"cursor":history_cursor}),
    )?;
    assert_eq!(old_history["items"][0]["seq"], 1);
    // Schema readback is lossless: a variant tag without fields is content.
    let definition = json!({"carrier":"trial","report":{},"plan":{"kind":{"presence":"required",
        "type":{"variant":{"guard":{},"confirmation":{"alpha":{"type":"float","presence":"required"}}}}}}});
    session.json(
        "frontier.patch",
        json!({"frontier":"latency","ops":[{"op":"define","schema":"attempt","definition":definition}]}),
    )?;
    let read = session.json("schema.read", json!({"schema":"attempt"}))?;
    assert_eq!(read["definitions"][0]["definition"], definition);
    assert_eq!(
        read["writing"],
        json!([
            r#"plan.kind: {"tag":"confirmation","value":{"alpha":0.5}} or {"tag":"guard","value":{}}"#
        ])
    );
    let text = session.call("schema.read", json!({"schema":"attempt"}))?;
    assert!(
        text["content"][0]["text"]
            .as_str()
            .is_some_and(|text| text.contains(r#""guard":{}"#) && text.contains(r#""report":{}"#)),
        "{text}"
    );
    session.json("frontier.patch",json!({"frontier":"latency","ops":[{"op":"conclude","lesson":"Cache parsing avoids repeated work"}]}))?;
    // A concluded frontier still takes annotations; an inapplicable section is ignored, named.
    session.json("frontier.patch",json!({"frontier":"latency","ops":[{"op":"annotate","work":"parse.cache#1","text":"Outputs diffed byte for byte."}]}))?;
    let packet = session.json(
        "record.read",
        json!({"frontier":"latency","work":"parse.cache#1","sections":["annotations","members"]}),
    )?;
    assert_eq!(
        packet["annotations"]["items"][0]["text"],
        "Outputs diffed byte for byte."
    );
    assert!(
        packet["ignored"][0]
            .as_str()
            .is_some_and(|w| w.contains("members")),
        "{packet}"
    );
    let search = session.json(
        "frontier.status",
        json!({"scope":"project","filter":{"text":"Cache parsing"}}),
    )?;
    let rows = search["items"].as_array().ok_or("missing search rows")?;
    assert!(
        rows.iter()
            .any(|row| row["frontier"] == "latency" && row["record"] == "parse.cache"),
        "{search}"
    );
    Ok(())
}

#[test]
fn timestamped_jsonl_import() -> Result {
    let cell = TestCell::new("spinner-import")?;
    let file = cell.join("patches.jsonl");
    let path = file.to_str().ok_or("non-UTF-8 path")?;
    std::fs::write(
        &file,
        concat!(
            "{\"at\":\"2025-01-01T00:00:00Z\",\"ops\":[{\"op\":\"frontier\",\"objective\":\"Synthetic inquiry\"}]}\n",
            "{\"at\":\"2025-01-02T00:00:00Z\",\"ops\":[{\"op\":\"iset\",\"title\":\"T\",\"slug\":\"cache\"}]}\n",
        ),
    )?;
    let output = cli(&cell, &["import", "--frontier", "imported", path])?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let store = fidget_spinner::store::open(&cell.join("fidget-spinner/ledger.sqlite3"))?;
    let address = fidget_spinner::store::Address {
        project: fidget_spinner::store::project_root(cell.path())?,
        slug: "imported".parse()?,
    };
    let mut ledger = fidget_spinner::store::Ledger::default();
    store.refresh(&address, &mut ledger)?;
    let history = store.history_at(
        ledger.id().ok_or("missing imported ledger")?,
        ledger.seq(),
        0,
        20,
    )?;
    let times = history
        .iter()
        .map(|entry| {
            entry
                .at
                .format(&time::format_description::well_known::Rfc3339)
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(times, ["2025-01-02T00:00:00Z", "2025-01-01T00:00:00Z"]);
    assert_eq!(ledger.state().work().len(), 1);
    assert!(
        !cli(&cell, &["import", "--frontier", "imported", path])?
            .status
            .success()
    );
    std::fs::write(
        &file,
        "{\"at\":\"2025-01-01T00:00:00Z\",\"ops\":[{\"op\":\"frontier\",\"objective\":\"o\",\"legacy_through\":1}]}\n",
    )?;
    assert!(
        !cli(&cell, &["import", "--frontier", "forged", path])?
            .status
            .success()
    );
    Ok(())
}

/// A format-21 store holding one FS 3 frontier.
fn fs3_store(cell: &TestCell, patches: &[Value]) -> Result<Connection> {
    let path = cell.join("fidget-spinner/ledger.sqlite3");
    std::fs::create_dir_all(path.parent().ok_or("store path has no parent")?)?;
    let connection = Connection::open(&path)?;
    connection.execute_batch(
        "CREATE TABLE frontiers (id INTEGER PRIMARY KEY, project TEXT NOT NULL, slug TEXT NOT NULL, UNIQUE(project,slug));
         CREATE TABLE patches (seq INTEGER PRIMARY KEY, frontier_id INTEGER NOT NULL REFERENCES frontiers(id), at TEXT NOT NULL, ops TEXT NOT NULL);
         CREATE INDEX patches_frontier_seq ON patches(frontier_id,seq);
         PRAGMA user_version = 21;",
    )?;
    let project = fidget_spinner::store::project_root(cell.path())?;
    connection.execute(
        "INSERT INTO frontiers VALUES (1, ?1, 'legacy')",
        [project.as_path().to_str().ok_or("non-UTF-8 project")?],
    )?;
    for (seq, patch) in (1_i64..).zip(patches) {
        connection.execute(
            "INSERT INTO patches VALUES (?1, 1, ?2, ?3)",
            params![seq, format!("2026-01-0{seq}T00:00:00Z"), patch.to_string()],
        )?;
    }
    Ok(connection)
}

fn version(connection: &Connection) -> Result<i64> {
    Ok(connection.query_row("PRAGMA user_version", [], |row| row.get(0))?)
}

#[test]
fn fs3_store_migrates_in_place() -> Result {
    let legacy = [
        json!([
            {"op":"frontier","objective":"legacy"},
            {"op":"hypothesis","slug":"h","title":"H","claim":"c"},
            {"op":"trial","hypothesis":"h","note":"first"},
            {"op":"accept","hypothesis":"h","rationale":"held"}
        ]),
        json!([
            {"op":"trial","hypothesis":"h","note":"second"},
            {"op":"reject","hypothesis":"h","rationale":"overturned"}
        ]),
    ];
    let cell = TestCell::new("spinner-fs3")?;
    let connection = fs3_store(&cell, &legacy)?;
    let output = cli(
        &cell,
        &[
            "read",
            "--frontier",
            "legacy",
            "--work",
            "h",
            "--render",
            "json",
        ],
    )?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let packet: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(version(&connection)?, fidget_spinner::store::FORMAT);
    assert!(
        packet["head"]["schema"]
            .as_str()
            .is_some_and(|s| s.starts_with("fs3.hypothesis@"))
    );
    assert_eq!(packet["judgment"]["answer"]["disposition"], "rejected");
    assert_eq!(packet["prior_judgments"]["total"], 1);
    assert_eq!(packet["members"]["total"], 2);
    // An FS 3 operation the migration does not know refuses it whole.
    let cell = TestCell::new("spinner-fs3-refused")?;
    let mut refused = legacy.to_vec();
    refused.push(json!([{"op":"waive","finding":"unbacked/h","note":"n"}]));
    let connection = fs3_store(&cell, &refused)?;
    let output = cli(&cell, &["status", "--frontier", "legacy"])?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("FS 3 patch 3"));
    assert_eq!(version(&connection)?, 21);
    let first: String =
        connection.query_row("SELECT ops FROM patches WHERE seq=1", [], |row| row.get(0))?;
    assert_eq!(serde_json::from_str::<Value>(&first)?, legacy[0]);
    Ok(())
}
