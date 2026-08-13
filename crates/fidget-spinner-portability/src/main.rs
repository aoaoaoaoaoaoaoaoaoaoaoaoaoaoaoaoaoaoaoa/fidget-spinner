use std::{
    env, fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};

const LOOPBACK: &str = "127.0.0.1";

fn main() -> Result<()> {
    let mode = env::args().nth(1).unwrap_or_else(|| "lifecycle".to_owned());
    ensure!(mode == "lifecycle", "unknown witness mode `{mode}`");
    prove_lifecycle()
}

fn prove_lifecycle() -> Result<()> {
    let workspace = workspace_root()?;
    let cell = tempfile::tempdir().context("create isolated lifecycle cell")?;
    let prefix = cell.path().join("prefix");
    let isolation = Isolation::raise(cell.path())?;

    let mut install = Command::new(cargo());
    let _ = install
        .current_dir(&workspace)
        .args(["install", "--locked", "--force", "--root"])
        .arg(&prefix)
        .arg("--path")
        .arg(workspace.join("crates/fidget-spinner-cli"));
    let _ = checked("install source release", &mut install)?;

    let binary = prefix
        .join("bin")
        .join(format!("fidget-spinner-cli{}", env::consts::EXE_SUFFIX));
    ensure!(
        binary.is_file(),
        "installed binary is absent: {}",
        binary.display()
    );
    prove_identity(&binary, &isolation)?;
    prove_project_and_skills(&binary, &isolation)?;
    prove_navigator(&binary, &isolation)?;

    let sentinel = isolation
        .state_root
        .join("fidget-spinner/projects/uninstall-must-preserve");
    fs::write(&sentinel, b"ledger state\n").context("write state sentinel")?;

    let mut uninstall = Command::new(cargo());
    let _ = uninstall
        .args(["uninstall", "--root"])
        .arg(&prefix)
        .arg("fidget-spinner-cli");
    let _ = checked("uninstall source release", &mut uninstall)?;
    ensure!(!binary.exists(), "binary survived Cargo uninstall");
    ensure!(sentinel.is_file(), "Cargo uninstall removed user state");
    println!("Fidget Spinner native lifecycle passed");
    Ok(())
}

fn workspace_root() -> Result<PathBuf> {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .context("portability crate is not nested beneath the workspace root")
}

fn cargo() -> PathBuf {
    env::var_os("CARGO").map_or_else(
        || PathBuf::from(format!("cargo{}", env::consts::EXE_SUFFIX)),
        PathBuf::from,
    )
}

fn prove_identity(binary: &Path, isolation: &Isolation) -> Result<()> {
    let mut version = isolation.command(binary);
    let _ = version.arg("--version");
    let version = checked("query installed version", &mut version)?;
    let version = String::from_utf8(version.stdout).context("version output is not UTF-8")?;
    ensure!(
        version.trim() == format!("fidget-spinner-cli {}", env!("CARGO_PKG_VERSION")),
        "installed binary reported an alien version: {version:?}"
    );

    let mut help = isolation.command(binary);
    let _ = help.arg("--help");
    let help = checked("query installed help", &mut help)?;
    let help = String::from_utf8(help.stdout).context("help output is not UTF-8")?;
    ensure!(
        help.contains("local navigator"),
        "installed help lost the navigator surface"
    );

    let mut skills = isolation.command(binary);
    let _ = skills.args(["skill", "list"]);
    let skills = checked("list bundled skills", &mut skills)?;
    let skills = String::from_utf8(skills.stdout).context("skill list is not UTF-8")?;
    ensure!(
        skills.contains("fidget-spinner") && skills.contains("frontier-loop"),
        "installed binary lost bundled skills"
    );
    Ok(())
}

fn prove_project_and_skills(binary: &Path, isolation: &Isolation) -> Result<()> {
    let project = isolation.root.join("project with spaces");
    fs::create_dir(&project).context("create witness project")?;

    let mut init = isolation.command(binary);
    let _ = init
        .args(["init", "--project"])
        .arg(&project)
        .args(["--name", "portability"]);
    let init = checked("initialize witness project", &mut init)?;
    ensure!(
        String::from_utf8_lossy(&init.stdout).contains("portability"),
        "project initialization omitted its identity"
    );

    let mut status = isolation.command(binary);
    let _ = status
        .args(["project", "status", "--project"])
        .arg(&project);
    let status = checked("reopen witness project", &mut status)?;
    ensure!(
        String::from_utf8_lossy(&status.stdout).contains("portability"),
        "reopened project lost its identity"
    );

    let skill_root = isolation.root.join("installed skills");
    let mut install_skills = isolation.command(binary);
    let _ = install_skills
        .args(["skill", "install", "--destination"])
        .arg(&skill_root);
    let _ = checked("install bundled skills", &mut install_skills)?;
    for skill in ["fidget-spinner", "frontier-loop"] {
        ensure!(
            skill_root.join(skill).join("SKILL.md").is_file(),
            "bundled skill `{skill}` was not installed"
        );
    }
    Ok(())
}

fn prove_navigator(binary: &Path, isolation: &Isolation) -> Result<()> {
    let port = vacant_port()?;
    let bind = format!("{LOOPBACK}:{port}");
    let mut command = isolation.command(binary);
    let _ = command
        .args(["ui", "serve", "--bind", &bind])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let child = command.spawn().context("launch installed navigator")?;
    let mut navigator = ChildGuard::new(child);

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = navigator.try_wait()? {
            bail!("navigator exited before first contact with {status}");
        }
        match TcpStream::connect((LOOPBACK, port)) {
            Ok(stream) => {
                drop(stream);
                break;
            }
            Err(_) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(error) => return Err(error).context("navigator did not bind before deadline"),
        }
    }

    let mut stream = TcpStream::connect((LOOPBACK, port)).context("connect to navigator")?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .context("bound navigator read timeout")?;
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .context("bound navigator write timeout")?;
    write!(
        stream,
        "GET / HTTP/1.1\r\nHost: {bind}\r\nConnection: close\r\n\r\n"
    )
    .context("send navigator request")?;
    let mut response = Vec::new();
    let _bytes_read = stream
        .read_to_end(&mut response)
        .context("read navigator response")?;
    let response = String::from_utf8(response).context("navigator response is not UTF-8")?;
    ensure!(
        response.starts_with("HTTP/1.1 200 OK"),
        "navigator first contact was not HTTP 200:\n{response}"
    );
    ensure!(
        response.contains("Fidget Spinner"),
        "navigator first contact lost product identity"
    );
    navigator.stop()?;
    Ok(())
}

fn vacant_port() -> Result<u16> {
    let listener = TcpListener::bind((LOOPBACK, 0)).context("reserve witness port")?;
    Ok(listener
        .local_addr()
        .context("read witness listener address")?
        .port())
}

fn checked(label: &str, command: &mut Command) -> Result<Output> {
    let output = command
        .output()
        .with_context(|| format!("execute {label}"))?;
    if output.status.success() {
        return Ok(output);
    }
    bail!(
        "{label} failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

struct Isolation {
    root: PathBuf,
    home: PathBuf,
    state_root: PathBuf,
}

impl Isolation {
    fn raise(root: &Path) -> Result<Self> {
        let isolation = Self {
            root: root.to_path_buf(),
            home: root.join("home with spaces"),
            state_root: root.join("state root"),
        };
        for path in [
            isolation.home.clone(),
            isolation.state_root.clone(),
            isolation.root.join("config root"),
            isolation.root.join("data root"),
            isolation.root.join("cache root"),
            isolation.root.join("appdata roaming"),
            isolation.root.join("appdata local"),
        ] {
            fs::create_dir_all(&path)
                .with_context(|| format!("create isolated path {}", path.display()))?;
        }
        Ok(isolation)
    }

    fn command(&self, binary: &Path) -> Command {
        let mut command = Command::new(binary);
        let _ = command
            .env("HOME", &self.home)
            .env("USERPROFILE", &self.home)
            .env("XDG_CONFIG_HOME", self.root.join("config root"))
            .env("XDG_STATE_HOME", &self.state_root)
            .env("XDG_DATA_HOME", self.root.join("data root"))
            .env("XDG_CACHE_HOME", self.root.join("cache root"))
            .env("APPDATA", self.root.join("appdata roaming"))
            .env("LOCALAPPDATA", self.root.join("appdata local"))
            .env("FIDGET_SPINNER_STATE_HOME", &self.state_root);
        command
    }
}

struct ChildGuard(Option<Child>);

impl ChildGuard {
    const fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn try_wait(&mut self) -> Result<Option<std::process::ExitStatus>> {
        self.0
            .as_mut()
            .context("navigator process was already reaped")?
            .try_wait()
            .context("poll navigator process")
    }

    fn stop(&mut self) -> Result<()> {
        let mut child = self
            .0
            .take()
            .context("navigator process was already reaped")?;
        if child
            .try_wait()
            .context("poll navigator before stop")?
            .is_none()
        {
            child.kill().context("stop navigator")?;
        }
        let _status = child.wait().context("reap navigator")?;
        Ok(())
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _kill_result = child.kill();
            let _wait_result = child.wait();
        }
    }
}
