use anyhow::Result;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

fn log_dir() -> PathBuf {
    dirs::home_dir()
        .expect("home dir")
        .join(".autonomic")
        .join("state")
        .join("supervisor")
        .join("logs")
}

pub fn list_logs() -> Result<Vec<String>> {
    let dir = log_dir();
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut names = Vec::new();
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map_or(false, |e| e == "log") {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                names.push(stem.to_string());
            }
        }
    }
    names.sort();
    Ok(names)
}

pub fn print_log(name: &str) -> Result<()> {
    let path = log_dir().join(format!("{name}.log"));
    if !path.exists() {
        anyhow::bail!("no log found for '{name}'. Available: {}", list_logs()?.join(", "));
    }
    let content = fs::read_to_string(&path)?;
    print!("{content}");
    Ok(())
}

pub fn follow_log(name: &str) -> Result<()> {
    let path = log_dir().join(format!("{name}.log"));
    if !path.exists() {
        anyhow::bail!("no log found for '{name}'. Available: {}", list_logs()?.join(", "));
    }
    let file = fs::File::open(&path)?;
    let mut reader = BufReader::new(file);
    let mut buf = String::new();
    loop {
        buf.clear();
        let bytes = reader.read_line(&mut buf)?;
        if bytes == 0 {
            thread::sleep(Duration::from_millis(200));
            continue;
        }
        print!("{buf}");
    }
}
