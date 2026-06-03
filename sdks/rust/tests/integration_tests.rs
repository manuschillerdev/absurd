// Integration tests for Absurd SDK
// These tests require a running PostgreSQL database with Absurd schema

use absurd::{Absurd, AwaitEventOptions, Error, Json, SpawnOptions, TaskOptions};
use serde_json::json;
use std::env;
use std::time::Duration;
use tokio_postgres::NoTls;
use uuid::Uuid;

fn get_test_db_url() -> String {
    env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql://localhost/absurd_test".to_string())
}

#[tokio::test]
#[ignore] // Requires database setup
async fn test_basic_task_execution() -> Result<(), Box<dyn std::error::Error>> {
    let absurd = Absurd::with_queue(&get_test_db_url(), "test").await?;
    absurd.create_queue(None).await?;

    absurd.register_task(
        TaskOptions::new("test-task").with_queue("test"),
        |params, mut ctx| Box::pin(async move {
            let value = params["value"].as_i64().unwrap();

            let doubled = ctx.step("double", || {
                let v = value;
                Box::pin(async move { Ok(v * 2) })
            }).await?;

            Ok(json!({ "result": doubled }))
        })
    );

    let spawn_result = absurd.spawn(
        "test-task",
        json!({ "value": 21 }),
        Default::default()
    ).await?;

    assert!(spawn_result.created);

    let count = absurd.work_batch("test-worker", 30, 1).await?;
    assert_eq!(count, 1);

    // Cleanup after test — TTL 0 removes everything completed
    absurd.cleanup(0, None).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_idempotency() -> Result<(), Box<dyn std::error::Error>> {
    let absurd = Absurd::with_queue(&get_test_db_url(), "test").await?;
    absurd.create_queue(None).await?;

    let idempotency_key = format!("unique-key-{}", uuid::Uuid::new_v4());

    let result1 = absurd.spawn(
        "test-task",
        json!({ "value": 42 }),
        SpawnOptions {
            idempotency_key: Some(idempotency_key.clone()),
            queue: Some("test".to_string()),
            ..Default::default()
        }
    ).await?;

    let result2 = absurd.spawn(
        "test-task",
        json!({ "value": 42 }),
        SpawnOptions {
            idempotency_key: Some(idempotency_key),
            queue: Some("test".to_string()),
            ..Default::default()
        }
    ).await?;

    assert_eq!(result1.task_id, result2.task_id);
    assert!(result1.created);
    assert!(!result2.created);

    absurd.cleanup(0, None).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_event_emission() -> Result<(), Box<dyn std::error::Error>> {
    let absurd = Absurd::with_queue(&get_test_db_url(), "test").await?;
    absurd.create_queue(None).await?;

    absurd.emit_event(
        "test.event",
        Some(json!({ "data": "test" })),
        Some("test")
    ).await?;

    // Clean up emitted events
    absurd.cleanup(0, None).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_queue_operations() -> Result<(), Box<dyn std::error::Error>> {
    let absurd = Absurd::with_queue(&get_test_db_url(), "test-ops").await?;

    absurd.create_queue(Some("test-ops")).await?;

    let queues = absurd.list_queues().await?;
    assert!(queues.contains(&"test-ops".to_string()));

    absurd.drop_queue(Some("test-ops")).await?;

    let queues = absurd.list_queues().await?;
    assert!(!queues.contains(&"test-ops".to_string()));

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    let absurd = Absurd::with_queue(&get_test_db_url(), "test").await?;
    absurd.create_queue(None).await?;

    absurd.register_task(
        TaskOptions::new("cleanup-test-task").with_queue("test"),
        |_, _ctx| Box::pin(async move {
            Ok(json!({ "status": "done" }))
        })
    );

    // Spawn and execute a task so there is data to clean up
    absurd.spawn(
        "cleanup-test-task",
        json!({}),
        Default::default()
    ).await?;

    absurd.work_batch("test-worker", 30, 1).await?;

    // TTL 0 should remove all completed tasks and events immediately
    absurd.cleanup(0, None).await?;

    // TTL 30 is a no-op here since nothing is older than 30 days,
    // but it should not error
    absurd.cleanup(30, None).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_await_event_legacy_timeout_fallback() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = get_test_db_url();
    let queue = format!("test_legacy_timeout_{}", Uuid::new_v4().simple());
    let absurd = Absurd::with_queue(&database_url, &queue).await?;
    absurd.create_queue(None).await?;

    let event_name = format!("legacy_timeout_{}", Uuid::new_v4().simple());
    absurd.register_task(
        TaskOptions::new("legacy-timeout").with_queue(&queue),
        move |_, mut ctx| {
            let event_name = event_name.clone();
            Box::pin(async move {
                match ctx.await_event(
                    &event_name,
                    AwaitEventOptions::new().with_step_name("wait").with_timeout(1),
                ).await {
                    Err(Error::EventTimeout(_, _)) => Ok(json!({ "stage": "timed-out" })),
                    Ok(payload) => Ok(json!({ "stage": "event", "payload": payload })),
                    Err(err) => Err(err),
                }
            })
        }
    );

    let spawned = absurd.spawn("legacy-timeout", Json::Null, Default::default()).await?;

    absurd.work_batch("worker-timeout", 30, 1).await?;
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    absurd.work_batch("worker-timeout", 30, 1).await?;

    let (state, completed_payload) = get_task_state(&database_url, &queue, spawned.task_id).await?;
    assert_eq!(state, "completed");
    assert_eq!(completed_payload, Some(json!({ "stage": "timed-out" })));

    absurd.drop_queue(None).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_await_event_timeout_checkpoint_preserves_progress() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = get_test_db_url();
    if !await_event_has_timed_out_column(&database_url).await? {
        eprintln!("skipping: absurd.await_event does not expose timed_out yet");
        return Ok(());
    }

    let queue = format!("test_timeout_checkpoint_{}", Uuid::new_v4().simple());
    let absurd = Absurd::with_queue(&database_url, &queue).await?;
    absurd.create_queue(None).await?;

    absurd.register_task(
        TaskOptions::new("timeout-loop").with_queue(&queue),
        |_, mut ctx| Box::pin(async move {
            let mut stages = Vec::new();

            for cycle in 0..2 {
                let event_name = format!("wake:{}", cycle);
                let step_name = format!("await-{}", cycle);
                match ctx.await_event(
                    &event_name,
                    AwaitEventOptions::new().with_step_name(step_name).with_timeout(1),
                ).await {
                    Err(Error::EventTimeout(_, _)) => stages.push(format!("timeout-{}", cycle)),
                    Ok(_) => stages.push(format!("event-{}", cycle)),
                    Err(err) => return Err(err),
                }
            }

            Ok(json!({ "stages": stages }))
        })
    );

    let spawned = absurd.spawn("timeout-loop", Json::Null, Default::default()).await?;

    absurd.work_batch("worker-timeout", 30, 1).await?;
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    absurd.work_batch("worker-timeout", 30, 1).await?;

    let (run_state, wake_event) = get_run_state(&database_url, &queue, spawned.run_id).await?;
    assert_eq!(run_state, "sleeping");
    assert_eq!(wake_event.as_deref(), Some("wake:1"));

    tokio::time::sleep(Duration::from_millis(1_200)).await;
    absurd.work_batch("worker-timeout", 30, 1).await?;

    let (state, completed_payload) = get_task_state(&database_url, &queue, spawned.task_id).await?;
    assert_eq!(state, "completed");
    assert_eq!(
        completed_payload,
        Some(json!({ "stages": ["timeout-0", "timeout-1"] }))
    );
    assert_eq!(count_waits(&database_url, &queue).await?, 0);

    absurd.drop_queue(None).await?;

    Ok(())
}

async fn await_event_has_timed_out_column(database_url: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let row = client.query_one(
        "SELECT pg_get_function_result('absurd.await_event(text, uuid, uuid, text, text, integer)'::regprocedure)",
        &[],
    ).await?;
    let result: String = row.get(0);

    Ok(result.contains("timed_out"))
}

async fn get_task_state(
    database_url: &str,
    queue: &str,
    task_id: Uuid,
) -> Result<(String, Option<Json>), Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let row = client.query_one(
        &format!("SELECT state, completed_payload FROM absurd.t_{} WHERE task_id = $1", queue),
        &[&task_id],
    ).await?;

    Ok((row.get(0), row.get(1)))
}

async fn get_run_state(
    database_url: &str,
    queue: &str,
    run_id: Uuid,
) -> Result<(String, Option<String>), Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let row = client.query_one(
        &format!("SELECT state, wake_event FROM absurd.r_{} WHERE run_id = $1", queue),
        &[&run_id],
    ).await?;

    Ok((row.get(0), row.get(1)))
}

async fn count_waits(database_url: &str, queue: &str) -> Result<i64, Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let row = client.query_one(
        &format!("SELECT COUNT(*) FROM absurd.w_{}", queue),
        &[],
    ).await?;

    Ok(row.get(0))
}
