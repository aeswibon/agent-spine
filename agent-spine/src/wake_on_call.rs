use std::path::PathBuf;
use std::time::Duration;
use tokio::process::{Child, Command};
use tracing::{info, warn};

const COMMON_PATHS: &[&str] = &[
    "/usr/local/bin/agent-brain",
    "/opt/homebrew/bin/agent-brain",
];

async fn find_brain() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("BRAIN_PATH") {
        let p = PathBuf::from(&path);
        if p.exists() {
            return Some(p);
        }
    }

    for path in COMMON_PATHS {
        let p = PathBuf::from(path);
        if p.exists() {
            return Some(p);
        }
    }

    if let Ok(output) = Command::new("which").arg("agent-brain").output().await {
        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !path.is_empty() {
            return Some(PathBuf::from(path));
        }
    }

    if let Ok(home) = std::env::var("HOME") {
        let cargo_bin = PathBuf::from(&home).join(".cargo/bin/agent-brain");
        if cargo_bin.exists() {
            return Some(cargo_bin);
        }

        let local_bin = PathBuf::from(&home).join(".local/bin/agent-brain");
        if local_bin.exists() {
            return Some(local_bin);
        }
    }

    None
}

pub async fn try_spawn_brain() -> Option<Child> {
    let path = find_brain().await?;
    info!("Found agent-brain at {:?}, spawning...", path);

    match Command::new(&path).arg("serve").kill_on_drop(true).spawn() {
        Ok(child) => {
            let pid = child.id().unwrap_or(0);
            info!("agent-brain spawned successfully (pid: {})", pid);
            Some(child)
        }
        Err(e) => {
            warn!("Failed to spawn agent-brain: {}", e);
            None
        }
    }
}

pub async fn wait_for_brain(max_attempts: u32) -> bool {
    for attempt in 0..max_attempts {
        if is_brain_alive().await {
            if attempt > 0 {
                info!("agent-brain ready after {} attempts", attempt + 1);
            }
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

async fn is_brain_alive() -> bool {
    #[cfg(unix)]
    {
        std::process::Command::new("pgrep")
            .arg("-x")
            .arg("agent-brain")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        true
    }
}
