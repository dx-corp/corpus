//! Integration tests for Merlin observation ingest and listing.
//! Gated on `CORPUS_TEST_DATABASE_URL`.

use corpus_core::dto::MerlinSegmentRequest;
use corpus_core::{db, merlin, tenant};
use uuid::Uuid;

#[tokio::test]
async fn merlin_segment_ingest_is_idempotent_and_rejects_digest_conflicts() {
    let Ok(url) = std::env::var("CORPUS_TEST_DATABASE_URL") else {
        eprintln!("CORPUS_TEST_DATABASE_URL unset; skipping Merlin integration test");
        return;
    };
    let pool = db::connect(&url).await.unwrap();
    db::migrate(&pool).await.unwrap();
    let tenant_id = Uuid::new_v4();
    let slug = format!("merlin-{}", &tenant_id.simple().to_string()[..8]);
    tenant::ensure_tenant(&pool, tenant_id, &slug, "Merlin bridge integration test")
        .await
        .unwrap();

    let req = MerlinSegmentRequest {
        schema_version: 1,
        host_name: "merlin-test-host".into(),
        segment: "merlin-events.20260804-010203.jsonl.gz".into(),
        segment_sha256: "A".repeat(64),
        events: vec![
            serde_json::json!({
                "schema_version": 1,
                "boot_id": "boot-a",
                "source_seq": 1,
                "event_id": "boot-a:1",
                "kind": "exec",
                "process_key": "boot-a:42:9",
                "ts": 1_722_744_003.25,
            }),
            serde_json::json!({
                "schema_version": 1,
                "boot_id": "boot-a",
                "source_seq": 2,
                "event_id": "boot-a:2",
                "kind": "connect",
                "ts": 1_722_744_004.25,
            }),
        ],
    };

    let first = merlin::ingest_segment(&pool, tenant_id, &req)
        .await
        .unwrap();
    assert_eq!(first.accepted_events, 2);
    assert_eq!(first.duplicate_events, 0);

    assert_eq!(first.receipt_version, 1);
    assert_eq!(first.segment_sha256, "a".repeat(64));
    assert_eq!(first.status, "accepted");
    let replay = merlin::ingest_segment(&pool, tenant_id, &req)
        .await
        .unwrap();
    assert_eq!(replay.segment_id, first.segment_id);
    assert_eq!(replay.accepted_events, 0);
    assert_eq!(replay.duplicate_events, 2);

    assert_eq!(replay.receipt_version, 1);
    assert_eq!(replay.status, "duplicate");
    let observations = merlin::list_observations(&pool, tenant_id, Some("merlin-test-host"), 100)
        .await
        .unwrap();
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[0].host_name, "merlin-test-host");
    assert_eq!(observations[0].payload["kind"], "connect");

    let mut conflict = req.clone();
    conflict.segment_sha256 = "B".repeat(64);
    assert!(merlin::ingest_segment(&pool, tenant_id, &conflict)
        .await
        .is_err());
}

#[tokio::test]
async fn merlin_full_batch_is_one_receipt_with_in_batch_duplicates_and_counts() {
    let Ok(url) = std::env::var("CORPUS_TEST_DATABASE_URL") else {
        eprintln!("CORPUS_TEST_DATABASE_URL unset; skipping Merlin integration test");
        return;
    };
    let pool = db::connect(&url).await.unwrap();
    db::migrate(&pool).await.unwrap();
    let tenant_id = Uuid::new_v4();
    let slug = format!("merlin-{}", &tenant_id.simple().to_string()[..8]);
    tenant::ensure_tenant(&pool, tenant_id, &slug, "Merlin batch integration test")
        .await
        .unwrap();

    // A bridge-sized batch of 1,000 events plus a repeat of the first
    // event_id, which must be counted once and reported as a duplicate.
    let mut events: Vec<serde_json::Value> = (1..=1_000)
        .map(|seq| {
            serde_json::json!({
                "boot_id": "boot-b",
                "source_seq": seq,
                "kind": "exec",
                "process_key": format!("boot-b:{seq}:1"),
                "sha256": "C".repeat(64),
                "ts": 1_722_744_000.5 + seq as f64,
            })
        })
        .collect();
    events.push(serde_json::json!({
        "boot_id": "boot-b",
        "source_seq": 1,
        "kind": "repeat",
    }));
    let first_half = MerlinSegmentRequest {
        schema_version: 1,
        host_name: "merlin-batch-host".into(),
        segment: "merlin-events.20261109-184009.jsonl.gz".into(),
        segment_sha256: "d".repeat(64),
        events: events[..600].to_vec(),
    };
    let second_half = MerlinSegmentRequest {
        events: events[600..].to_vec(),
        ..first_half.clone()
    };

    let first = merlin::ingest_segment(&pool, tenant_id, &first_half)
        .await
        .unwrap();
    assert_eq!(first.accepted_events, 600);
    assert_eq!(first.duplicate_events, 0);
    let second = merlin::ingest_segment(&pool, tenant_id, &second_half)
        .await
        .unwrap();
    assert_eq!(second.segment_id, first.segment_id);
    assert_eq!(second.accepted_events, 400);
    assert_eq!(second.duplicate_events, 1);
    assert_eq!(second.status, "accepted");

    let replay = merlin::ingest_segment(&pool, tenant_id, &second_half)
        .await
        .unwrap();
    assert_eq!(replay.accepted_events, 0);
    assert_eq!(replay.duplicate_events, 401);
    assert_eq!(replay.status, "duplicate");

    let (event_count, stored): (i64, i64) = sqlx::query_as(
        "SELECT s.event_count,
                (SELECT COUNT(*) FROM merlin_observation o WHERE o.segment_id = s.id)
         FROM merlin_segment s WHERE s.id = $1",
    )
    .bind(first.segment_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(event_count, 1_000);
    assert_eq!(stored, 1_000);

    let observations = merlin::list_observations(&pool, tenant_id, Some("merlin-batch-host"), 1)
        .await
        .unwrap();
    assert_eq!(observations[0].event_id, "boot-b:1000");
    assert_eq!(observations[0].source_seq, Some(1_000));
    assert_eq!(observations[0].kind, "exec");
    assert_eq!(
        observations[0].artifact_sha256.as_deref(),
        Some("c".repeat(64).as_str())
    );
    assert!(observations[0].observed_at.is_some());
}
