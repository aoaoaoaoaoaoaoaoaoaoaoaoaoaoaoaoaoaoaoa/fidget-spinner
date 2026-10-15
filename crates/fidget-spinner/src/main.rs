#![expect(
    unused_crate_dependencies,
    reason = "The library target owns the server dependencies."
)]

use std::{io::Read as _, path::PathBuf, process::ExitCode};

use clap::{Args, Parser, Subcommand};
use fidget_spinner::{
    inspect::{DecisionName, Section, View},
    mcp::{Fault, Server, Tool},
    store::{self, Address},
};
use serde_json::{Value, json};
use trebuchet::Slug;

#[derive(Parser)]
#[command(
    version,
    about = "Frontier ledger, MCP server and command-line inspection"
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
struct Lens {
    #[arg(long,conflicts_with_all=["review","docket","scope"])]
    under: Option<Slug>,
    #[arg(long,conflicts_with_all=["under","docket","scope"])]
    review: Option<Slug>,
    #[arg(long,conflicts_with_all=["under","review","scope"])]
    docket: Option<Slug>,
    #[arg(long,value_parser=["project"],conflicts_with_all=["under","review","docket"])]
    scope: Option<String>,
    #[arg(long,value_delimiter=',',value_parser=["waiting","ready","running","tested","settled","completed"])]
    state: Option<Vec<String>>,
    #[arg(long, value_enum, value_delimiter = ',')]
    disposition: Option<Vec<DecisionName>>,
    #[arg(long)]
    owner: Option<String>,
    #[arg(long)]
    lane: Option<String>,
    #[arg(long)]
    text: Option<String>,
}

impl Lens {
    fn insert(self, args: &mut Value) {
        args["scope"] = self.scope.map_or_else(
            || json!({"under":self.under,"review":self.review,"docket":self.docket}),
            Value::String,
        );
        args["filter"] = json!({"state":self.state,"disposition":self.disposition,
            "owner":self.owner,"lane":self.lane,"text":self.text});
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
        lens: Lens,
        #[command(flatten)]
        window: Window,
    },
    /// Read a hypothesis packet; --review selects a review packet.
    Read {
        #[command(flatten)]
        target: Target,
        record: Slug,
        #[arg(long)]
        review: bool,
        #[arg(long, value_enum, value_delimiter = ',')]
        sections: Option<Vec<Section>>,
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
    let cwd = std::env::current_dir()?;
    let mut server = Server::new(path.clone(), &cwd)?;
    let (tool, arguments) = match cli.command {
        Command::Mcp {
            command: McpCommand::Serve,
        } => return Ok(server.serve()?),
        Command::Patch {
            target,
            dry_run,
            file,
        } => {
            let source = source(file)?;
            let patch: Value = serde_json::from_str(&source)?;
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
            lens,
            window,
        } => {
            let mut args = target.arguments();
            args["view"] = json!(view);
            lens.insert(&mut args);
            window.insert(&mut args);
            (Tool::Status, args)
        }
        Command::Read {
            target,
            record,
            review,
            sections,
            window,
        } => {
            let mut args = target.arguments();
            args[if review { "review" } else { "hypothesis" }] = json!(record);
            args["sections"] = json!(sections);
            window.insert(&mut args);
            (
                if review {
                    Tool::Review
                } else {
                    Tool::Hypothesis
                },
                args,
            )
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
            let project = store::project_root(target.project.as_deref().unwrap_or(&cwd))?;
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
    let _ = std::io::stdin().read_to_string(&mut source)?;
    Ok(source)
}
