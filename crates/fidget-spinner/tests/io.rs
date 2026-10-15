#![expect(
    unused_crate_dependencies,
    reason = "Integration tests exercise selected public boundary dependencies."
)]

use std::{
    error::Error,
    io::{self, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
};

use libmcp::{FrameLimit, FrameReadOutcome, read_frame_blocking, write_frame_blocking};
use libmcp_testkit::TestCell;
use serde_json::{Value, json};

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

#[test]
fn mcp_inquiry_session() -> Result {
    let mut session = Session::new()?;
    let tools = session.rpc("tools/list", json!({}))?;
    assert_eq!(tools["tools"].as_array().map(Vec::len), Some(6));
    let receipt=session.json("frontier.patch",json!({"frontier":"latency","ops":[
        {"op":"frontier","objective":"Reduce latency without changing behavior"},
        {"op":"regime","contract":"one worker; fixed cases","metrics":[{"key":"elapsed","unit":"ms","better":"lower","kpi":true}]},
        {"op":"baseline","slug":"initial","readings":{"elapsed":10}},
        {"op":"accept","baseline":"initial","rationale":"Measured starting point"}
    ]}))?;
    assert_eq!(receipt["seq"], 1);
    let patched=session.json("frontier.patch",json!({"frontier":"latency","ops":[
        {"op":"hypothesis","slug":"parse.cache","title":"Cache parsing","claim":"Reduce repeated work"},
        {"op":"trial","hypothesis":"parse.cache","note":"Identical outputs; 20% faster","readings":{"elapsed":8}},
        {"op":"hypothesis","slug":"discarded","title":"Secret removed title"},
        {"op":"rename","hypothesis":"discarded","to":"renamed"},
        {"op":"remove","hypothesis":"renamed"},
        {"op":"hypothesis","slug":"renamed","title":"A distinct record"}
    ]}))?;
    assert_eq!(patched["changes"]["tested"]["after"], 1);
    let status = session.json("frontier.status", json!({}))?;
    assert_eq!(status["frontier"], "latency");
    assert_eq!(status["counts"]["tested"], 1);
    assert_eq!(status["incumbent_baseline"]["slug"], "initial");
    let packet = session.json(
        "hypothesis.read",
        json!({"hypothesis":"parse.cache","sections":["trials"]}),
    )?;
    assert_eq!(
        packet["trials"]["items"][0]["comparisons"]["elapsed"]["ratio"],
        0.8
    );
    let history = session.json("frontier.history", json!({"seq":2,"detail":"full"}))?;
    assert!(!history.to_string().contains("Secret removed title"));
    assert_eq!(
        history["items"][2],
        json!({"op":"hypothesis","kind":"hypothesis"})
    );
    assert_eq!(history["items"][5]["title"], "A distinct record");
    let page = session.json("frontier.status", json!({"view":"hypotheses","limit":1}))?;
    let cursor = page["next_cursor"]
        .as_str()
        .ok_or("missing status cursor")?;
    let history_page = session.json("frontier.history", json!({"limit":1}))?;
    let history_cursor = history_page["next_cursor"]
        .as_str()
        .ok_or("missing history cursor")?;
    let _ = session.json(
        "frontier.patch",
        json!({"frontier":"latency","ops":[{"op":"remove","hypothesis":"renamed"}]}),
    )?;
    let stale = session.call(
        "frontier.status",
        json!({"view":"hypotheses","limit":1,"cursor":cursor}),
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
    let _=session.json("frontier.patch",json!({"frontier":"latency","ops":[{"op":"conclude","lesson":"Cache parsing avoids repeated work"}]}))?;
    let search = session.json(
        "frontier.status",
        json!({"scope":"project","filter":{"text":"Cache"}}),
    )?;
    assert_eq!(search["items"][0]["frontier"], "latency");
    assert_eq!(search["items"][0]["kind"], "hypothesis");
    Ok(())
}

#[test]
fn timestamped_jsonl_import() -> Result {
    let cell = TestCell::new("spinner-import")?;
    let file = cell.join("patches.jsonl");
    std::fs::write(
        &file,
        concat!(
            "{\"at\":\"2025-01-01T00:00:00Z\",\"ops\":[{\"op\":\"frontier\",\"objective\":\"Synthetic inquiry\"}]}\n",
            "{\"at\":\"2025-01-02T00:00:00Z\",\"ops\":[{\"op\":\"hypothesis\",\"slug\":\"cache\",\"title\":\"Cache\"}]}\n",
        ),
    )?;
    let import = || {
        Command::new(env!("CARGO_BIN_EXE_fidget-spinner"))
            .args(["import", "--frontier", "imported"])
            .arg(&file)
            .env("XDG_STATE_HOME", cell.path())
            .current_dir(cell.path())
            .output()
    };
    let output = import()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let path = cell.join("fidget-spinner/ledger.sqlite3");
    let store = fidget_spinner::store::open(&path)?;
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
    assert_eq!(history.len(), 2);
    assert_eq!(
        history[0]
            .at
            .format(&time::format_description::well_known::Rfc3339)?,
        "2025-01-02T00:00:00Z"
    );
    assert_eq!(
        history[1]
            .at
            .format(&time::format_description::well_known::Rfc3339)?,
        "2025-01-01T00:00:00Z"
    );
    assert_eq!(ledger.state().hypotheses().len(), 1);
    assert!(!import()?.status.success());
    Ok(())
}
