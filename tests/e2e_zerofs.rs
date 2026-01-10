//! E2E test simulating zerofs/SlateDB workload pattern
//!
//! Run with: cargo test --test e2e_zerofs -- --nocapture
//!
//! Requires:
//! - Proxy running on localhost:19000
//! - BUNNY_STORAGE_ZONE env var

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use futures::future::join_all;
use rand::Rng;
use std::time::{Duration, Instant};

const PROXY_URL: &str = "http://127.0.0.1:19000";

async fn create_s3_client() -> Client {
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            "test", "test", None, None, "test",
        ))
        .endpoint_url(PROXY_URL)
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .force_path_style(true)
        .build();

    aws_sdk_s3::Client::from_conf(s3_config)
}

fn random_data(size_mb: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..size_mb * 1024 * 1024).map(|_| rng.r#gen()).collect()
}

async fn put_object(client: &Client, bucket: &str, key: &str, data: Vec<u8>) -> Result<(), String> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(data))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("PUT failed: {}", e))
}

async fn get_object(client: &Client, bucket: &str, key: &str) -> Result<usize, String> {
    let result = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| format!("GET failed: {}", e))?;

    let bytes = result
        .body
        .collect()
        .await
        .map_err(|e| format!("Failed to read body: {}", e))?;

    Ok(bytes.into_bytes().len())
}

async fn list_objects(client: &Client, bucket: &str, prefix: &str) -> Result<usize, String> {
    let result = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await
        .map_err(|e| format!("LIST failed: {:?}", e))?;

    Ok(result.key_count().unwrap_or(0) as usize)
}

async fn delete_object(client: &Client, bucket: &str, key: &str) -> Result<(), String> {
    client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("DELETE failed: {}", e))
}

/// Simulate a single L0 compaction burst: 5 concurrent SST uploads
async fn simulate_compaction_burst(
    client: &Client,
    bucket: &str,
    burst_id: usize,
    sst_size_mb: usize,
) -> Result<Duration, String> {
    let start = Instant::now();

    let files: Vec<_> = (0..5)
        .map(|i| {
            let key = format!("zerofs-test/compacted/burst{:04}-{:02}.sst", burst_id, i);
            let data = random_data(sst_size_mb);
            (key, data)
        })
        .collect();

    let uploads: Vec<_> = files
        .into_iter()
        .map(|(key, data)| {
            let client = client.clone();
            let bucket = bucket.to_string();
            async move { put_object(&client, &bucket, &key, data).await }
        })
        .collect();

    let results = join_all(uploads).await;

    for (i, result) in results.iter().enumerate() {
        if let Err(e) = result {
            return Err(format!("Upload {} failed: {}", i, e));
        }
    }

    Ok(start.elapsed())
}

/// Simulate mixed workload: uploads + reads + lists
async fn simulate_mixed_workload(
    client: &Client,
    bucket: &str,
    duration_secs: u64,
    sst_size_mb: usize,
) -> Result<(usize, usize, usize), String> {
    let start = Instant::now();
    let mut burst_count = 0;
    let mut read_count = 0;
    let mut list_count = 0;

    while start.elapsed().as_secs() < duration_secs {
        simulate_compaction_burst(client, bucket, burst_count, sst_size_mb).await?;
        burst_count += 1;
        println!("  Burst {} completed", burst_count);

        for i in 0..3 {
            let key = format!(
                "zerofs-test/compacted/burst{:04}-{:02}.sst",
                burst_count.saturating_sub(1),
                i
            );
            if get_object(client, bucket, &key).await.is_ok() {
                read_count += 1;
            }
        }

        list_objects(client, bucket, "zerofs-test/").await?;
        list_count += 1;

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Ok((burst_count * 5, read_count, list_count))
}

#[tokio::test]
async fn test_zerofs_workload_pattern() {
    let bucket = match std::env::var("BUNNY_STORAGE_ZONE") {
        Ok(b) => b,
        Err(_) => {
            eprintln!("Skipping: BUNNY_STORAGE_ZONE not set");
            return;
        }
    };

    println!("\n=== ZeroFS Workload Simulation ===");
    println!("Bucket: {}", bucket);
    println!("SST size: 4MB");
    println!("Pattern: 5 concurrent uploads per burst");
    println!();

    let client = create_s3_client().await;

    println!("Running mixed workload for 30 seconds...");
    let start = Instant::now();

    match simulate_mixed_workload(&client, &bucket, 30, 4).await {
        Ok((uploads, reads, lists)) => {
            let elapsed = start.elapsed();
            println!();
            println!("=== Results ===");
            println!("Duration: {:.1}s", elapsed.as_secs_f64());
            println!("Uploads: {} ({} bursts)", uploads, uploads / 5);
            println!("Reads: {}", reads);
            println!("Lists: {}", lists);
            println!(
                "Upload throughput: {:.1} MB/s",
                (uploads * 4) as f64 / elapsed.as_secs_f64()
            );
            println!();
            println!("SUCCESS: Proxy handled zerofs workload pattern");
        }
        Err(e) => {
            panic!("FAIL: {}", e);
        }
    }

    println!("Cleaning up test files...");
    for burst in 0..100 {
        for i in 0..5 {
            let key = format!("zerofs-test/compacted/burst{:04}-{:02}.sst", burst, i);
            let _ = delete_object(&client, &bucket, &key).await;
        }
    }
}

#[tokio::test]
async fn test_large_concurrent_uploads() {
    let bucket = match std::env::var("BUNNY_STORAGE_ZONE") {
        Ok(b) => b,
        Err(_) => {
            eprintln!("Skipping: BUNNY_STORAGE_ZONE not set");
            return;
        }
    };

    println!("\n=== Large Concurrent Upload Test ===");
    println!("Bucket: {}", bucket);
    println!("File size: 8MB each");
    println!("Concurrent: 5 files");
    println!();

    let client = create_s3_client().await;

    let start = Instant::now();

    let uploads: Vec<_> = (0..5)
        .map(|i| {
            let client = client.clone();
            let bucket = bucket.clone();
            let data = random_data(8);
            async move {
                let key = format!("zerofs-test/large/file{}.sst", i);
                put_object(&client, &bucket, &key, data).await
            }
        })
        .collect();

    let results = join_all(uploads).await;
    let elapsed = start.elapsed();

    let mut success = 0;
    for (i, result) in results.iter().enumerate() {
        match result {
            Ok(_) => {
                println!("  Upload {}: OK", i);
                success += 1;
            }
            Err(e) => println!("  Upload {}: FAILED - {}", i, e),
        }
    }

    println!();
    println!("Duration: {:.1}s", elapsed.as_secs_f64());
    println!("Success: {}/5", success);
    println!(
        "Throughput: {:.1} MB/s",
        (success * 8) as f64 / elapsed.as_secs_f64()
    );

    for i in 0..5 {
        let key = format!("zerofs-test/large/file{}.sst", i);
        let _ = delete_object(&client, &bucket, &key).await;
    }

    assert_eq!(success, 5, "All uploads should succeed");
}
