use std::path::Path;
use std::time::Duration;

use agent_body_core::nats::subjects;
use agent_body_core::{ExecuteResult, SandboxExecute};
use futures::StreamExt;
use uuid::Uuid;

use crate::sandbox::SandboxResult;

/// Dispatch sandbox execution to agent-immune via JetStream and await the result.
pub async fn run_sandbox_via_jetstream(
    nats_url: &str,
    command: &str,
    cwd: Option<&Path>,
    timeout: Duration,
) -> Result<SandboxResult, String> {
    let client = match tokio::time::timeout(
        std::time::Duration::from_secs(3),
        async_nats::connect(nats_url),
    )
    .await
    {
        Ok(Ok(c)) => c,
        _ => return Err("NATS connect failed or timed out".to_string()),
    };

    let job_id = Uuid::now_v7().to_string();
    let msg_id = format!("sandbox-{job_id}");

    let mut sub = client
        .subscribe(subjects::EXECUTE_RESULT.to_string())
        .await
        .map_err(|e| format!("subscribe execute.result failed: {e}"))?;

    let job = SandboxExecute {
        msg_id: msg_id.clone(),
        job_id: job_id.clone(),
        command: command.to_string(),
        cwd: cwd.map(|p| p.display().to_string()),
    };

    let js = crate::jetstream::ensure_autonomic_stream(&client).await?;
    let bytes =
        serde_json::to_vec(&job).map_err(|e| format!("serialize sandbox job failed: {e}"))?;
    crate::jetstream::publish_dedup(&js, subjects::EXECUTE_SANDBOX, &msg_id, &bytes).await?;

    let result = tokio::time::timeout(timeout, async {
        while let Some(msg) = sub.next().await {
            let result: ExecuteResult = serde_json::from_slice(&msg.payload)
                .map_err(|e| format!("invalid execute result: {e}"))?;
            if result.job_id == job_id {
                return Ok(SandboxResult {
                    stdout: result.stdout,
                    stderr: result.stderr,
                    exit_code: result.exit_code,
                });
            }
        }
        Err("execute.result stream closed".to_string())
    })
    .await
    .map_err(|_| format!("sandbox JetStream timed out after {}s", timeout.as_secs()))??;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_job_ids_are_unique() {
        let id1 = Uuid::now_v7().to_string();
        let id2 = Uuid::now_v7().to_string();
        assert_ne!(id1, id2);
    }
}
