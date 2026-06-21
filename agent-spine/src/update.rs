use anyhow::{Context, Result, bail};
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

const REPO: &str = "autonomic-ai-dev/agent-spine";
const BINARY: &str = "agent-spine";

/// Detect the release artifact target triple.
fn detect_target() -> Option<&'static str> {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    match (os, arch) {
        ("macos", "aarch64") => Some("aarch64-apple-darwin"),
        ("macos", "x86_64") => Some("x86_64-apple-darwin"),
        ("linux", "x86_64") => Some("x86_64-unknown-linux-gnu"),
        ("linux", "aarch64") => Some("aarch64-unknown-linux-gnu"),
        ("windows", "x86_64") => Some("x86_64-pc-windows-msvc"),
        _ => None,
    }
}

fn parse_version(raw: &str) -> Vec<u32> {
    raw.trim()
        .trim_start_matches('v')
        .split('.')
        .filter_map(|part| part.parse::<u32>().ok())
        .collect()
}

fn version_is_newer(latest: &str, current: &str) -> bool {
    parse_version(latest) > parse_version(current)
}

/// Fetch the latest release tag from GitHub API.
fn fetch_latest_version() -> Result<String> {
    let url = format!("https://api.github.com/repos/{REPO}/releases/latest");
    let output = Command::new("curl")
        .args(["-fsSL", &url])
        .output()
        .context("failed to run curl, is curl installed?")?;
    if !output.status.success() {
        bail!("GitHub API request failed");
    }
    let body = String::from_utf8_lossy(&output.stdout);
    // Parse "tag_name":"v1.2.3" from JSON
    for line in body.lines() {
        if let Some(start) = line.find("\"tag_name\":\"") {
            let start = start + 12;
            if let Some(end) = line[start..].find('\"') {
                return Ok(line[start..start + end].to_string());
            }
        }
    }
    bail!("could not parse tag_name from GitHub API response");
}

#[cfg(target_os = "macos")]
fn codesign(path: &std::path::Path) {
    let _ = Command::new("xattr")
        .args(["-cr", &path.to_string_lossy()])
        .status();
    let _ = Command::new("codesign")
        .args(["--force", "--sign", "-", &path.to_string_lossy()])
        .status();
}

pub fn run_update(force: bool) -> Result<bool> {
    let current = env!("CARGO_PKG_VERSION");
    let latest = fetch_latest_version()?;
    let latest_ver = latest.trim_start_matches('v');

    if !force && !version_is_newer(latest_ver, current) {
        println!("{BINARY} already at latest version ({current})");
        return Ok(false);
    }

    let Some(target) = detect_target() else {
        bail!(
            "unsupported platform: {}-{}",
            std::env::consts::OS,
            std::env::consts::ARCH
        );
    };

    let url = format!("https://github.com/{REPO}/releases/latest/download/{BINARY}-{target}");
    let exe = std::env::current_exe().context("get current exe path")?;
    let tmp = exe.with_extension("download");
    let tmp_str = tmp.to_string_lossy().to_string();

    println!("Downloading {BINARY} v{latest}...");
    let status = Command::new("curl")
        .args(["-fsSL", &url, "-o", &tmp_str])
        .status()
        .context("failed to run curl")?;
    if !status.success() {
        bail!("download failed — release may not exist for this platform ({target})");
    }

    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))
        .context("set executable permissions")?;

    std::fs::rename(&tmp, &exe).context("replace binary")?;

    #[cfg(target_os = "macos")]
    codesign(&exe);

    println!("Updated {BINARY} from v{current} to v{latest}");
    Ok(true)
}
