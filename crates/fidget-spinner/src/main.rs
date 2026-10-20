#![expect(
    unused_crate_dependencies,
    reason = "The library target owns the server dependencies."
)]

use std::{
    io::Read as _,
    path::{Path, PathBuf},
    process::ExitCode,
};

use clap::{Args, Parser, Subcommand};
use fidget_spinner::{
    inspect::{Section, StateName, View},
    mcp::{Fault, Server, Tool},
    store::{self, Address},
};
use serde_json::{Value, json};
use trebuchet::Slug;

#[derive(Parser)]
#[command(
    version,
    about = "Inquiry ledger, MCP server and command-line inspection"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Args)]
struct Target {
    #[arg(long)]
    project: Option<PathBuf>,
    #[arg(long)]
    frontier: Option<Slug>,
    #[arg(long,default_value="porcelain",value_parser=["porcelain","json"])]
    render: String,
    #[arg(long,default_value="concise",value_parser=["concise","full"])]
    detail: String,
}

impl Target {
    fn arguments(self) -> Value {
        json!({"project":self.project,"frontier":self.frontier,"render":self.render,"detail":self.detail})
    }
}

#[derive(Args)]
struct Window {
    #[arg(long)]
    limit: Option<usize>,
    #[arg(long)]
    cursor: Option<String>,
}

impl Window {
    fn insert(self, args: &mut Value) {
        args["limit"] = json!(self.limit);
        args["cursor"] = json!(self.cursor);
    }
}

#[derive(Args)]
struct Selectors {
    /// Search every frontier of the project; requires --text, or --view frontiers.
    #[arg(long, value_parser = ["project"])]
    scope: Option<String>,
    /// Members of this iset at any depth.
    #[arg(long)]
    under: Option<String>,
    #[arg(long)]
    lane: Option<String>,
    #[arg(long, value_enum, value_delimiter = ',')]
    state: Option<Vec<StateName>>,
    #[arg(long)]
    owner: Option<String>,
    #[arg(long)]
    schema: Option<String>,
    #[arg(long)]
    text: Option<String>,
}

impl Selectors {
    fn insert(self, args: &mut Value) {
        args["scope"] = json!(self.scope);
        args["filter"] = json!({"under":self.under,"lane":self.lane,"state":self.state,
            "owner":self.owner,"schema":self.schema,"text":self.text});
    }
}

#[derive(Subcommand)]
enum Command {
    /// Serve MCP over stdio.
    Mcp {
        #[command(subcommand)]
        command: McpCommand,
    },
    /// Apply a JSON operation array or {`ops,dry_run`} from FILE or stdin.
    #[command(after_long_help=fidget_spinner::mcp::OPS_GRAMMAR)]
    Patch {
        #[command(flatten)]
        target: Target,
        #[arg(long)]
        dry_run: bool,
        file: Option<PathBuf>,
    },
    /// Inspect a frontier, or search every frontier of a project.
    Status {
        #[command(flatten)]
        target: Target,
        #[arg(long, value_enum, default_value = "summary")]
        view: View,
        #[command(flatten)]
        selectors: Selectors,
        #[command(flatten)]
        window: Window,
    },
    /// Read one trial or iset (--work) or baseline (--baseline).
    Read {
        #[command(flatten)]
        target: Target,
        #[arg(
            long,
            conflicts_with = "baseline",
            required_unless_present = "baseline"
        )]
        work: Option<String>,
        #[arg(long)]
        baseline: Option<String>,
        #[arg(long, value_enum, value_delimiter = ',')]
        sections: Option<Vec<Section>>,
        #[command(flatten)]
        window: Window,
    },
    /// Read a schema id's definitions and bound work, or the definition grammar.
    Schema {
        #[command(flatten)]
        target: Target,
        #[arg(long)]
        schema: Option<String>,
        #[command(flatten)]
        window: Window,
    },
    /// List patches, or read the operations at --seq with --detail full.
    History {
        #[command(flatten)]
        target: Target,
        #[arg(long)]
        seq: Option<i64>,
        #[command(flatten)]
        window: Window,
    },
    /// Import timestamped {at,ops} JSONL into a new frontier.
    Import {
        #[command(flatten)]
        target: Target,
        file: Option<PathBuf>,
    },
    /// Inspect process and store health.
    Health {
        #[command(flatten)]
        target: Target,
    },
    /// Serve the loopback navigator.
    Ui {
        #[command(subcommand)]
        command: UiCommand,
    },
}

#[derive(Subcommand)]
enum UiCommand {
    Serve {
        #[arg(long, default_value = "127.0.0.1:8913")]
        bind: std::net::SocketAddr,
    },
}

#[derive(Subcommand)]
enum McpCommand {
    Serve,
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("fidget-spinner: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), Fault> {
    let path = store::default_path()?;
    let cwd = Path::new(".");
    let mut server = Server::new(path.clone(), cwd);
    let (tool, arguments) = match cli.command {
        Command::Ui {
            command: UiCommand::Serve { bind },
        } => return fidget_spinner::ui::serve(path, bind),
        Command::Mcp {
            command: McpCommand::Serve,
        } => return Ok(server.serve()?),
        Command::Patch {
            target,
            dry_run,
            file,
        } => {
            let patch: Value = serde_json::from_str(&source(file)?)?;
            let mut args = target.arguments();
            match patch {
                Value::Array(ops) => {
                    args["ops"] = json!(ops);
                    args["dry_run"] = json!(dry_run);
                }
                Value::Object(fields) => {
                    for (key, value) in fields {
                        if !matches!(key.as_str(), "ops" | "dry_run") {
                            return Err(Fault::Arguments(format!("unknown patch field {key}")));
                        }
                        args[&key] = value;
                    }
                    if dry_run {
                        args["dry_run"] = json!(true);
                    }
                }
                _ => {
                    return Err(Fault::Arguments(
                        "patch must be an operation array or {ops,dry_run}".into(),
                    ));
                }
            }
            (Tool::Patch, args)
        }
        Command::Status {
            target,
            view,
            selectors,
            window,
        } => {
            let mut args = target.arguments();
            args["view"] = json!(view);
            selectors.insert(&mut args);
            window.insert(&mut args);
            (Tool::Status, args)
        }
        Command::Read {
            target,
            work,
            baseline,
            sections,
            window,
        } => {
            let mut args = target.arguments();
            args["work"] = json!(work);
            args["baseline"] = json!(baseline);
            args["sections"] = json!(sections);
            window.insert(&mut args);
            (Tool::Read, args)
        }
        Command::Schema {
            target,
            schema,
            window,
        } => {
            let mut args = target.arguments();
            args["schema"] = json!(schema);
            window.insert(&mut args);
            (Tool::Schema, args)
        }
        Command::History {
            target,
            seq,
            window,
        } => {
            let mut args = target.arguments();
            args["seq"] = json!(seq);
            window.insert(&mut args);
            (Tool::History, args)
        }
        Command::Import { target, file } => {
            let slug = target
                .frontier
                .ok_or_else(|| Fault::Arguments("import requires --frontier".into()))?;
            let project = target.project.as_deref().map_or_else(
                || {
                    store::project_root(cwd).map_err(|error| {
                        Fault::Arguments(format!("working directory: {error}; pass project"))
                    })
                },
                |project| Ok(store::project_root(project)?),
            )?;
            let count = store::import(
                &path,
                &Address {
                    project,
                    slug: slug.clone(),
                },
                &source(file)?,
            )?;
            if target.render == "json" {
                println!("{}", json!({"frontier":slug,"imported":count}));
            } else {
                println!("frontier {slug} · imported {count} patches");
            }
            return Ok(());
        }
        Command::Health { target } => (Tool::Health, target.arguments()),
    };
    let result = server.invoke(tool, arguments)?;
    if let Some(json) = result.get("structuredContent") {
        println!("{json}");
    } else if let Some(text) = result["content"][0]["text"].as_str() {
        println!("{text}");
    }
    Ok(())
}

fn source(file: Option<PathBuf>) -> Result<String, Fault> {
    if let Some(file) = file {
        return Ok(std::fs::read_to_string(file)?);
    }
    let mut source = String::new();
    std::io::stdin().read_to_string(&mut source)?;
    Ok(source)
}
