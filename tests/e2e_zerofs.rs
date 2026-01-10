//! E2E test simulating zerofs/SlateDB workload pattern
//!
//! Run with: cargo test --test e2e_zerofs -- --nocapture
//!
//! Requires:
//! - Proxy running on localhost:19000
//! - BUNNY_STORAGE_ZONE env var

use futures::future::join_all;
use rand::Rng;
use reqwest::Client;
use std::time::{Duration, Instant};

const PROXY_URL: &str = "http://127.0.0.1:19000";

fn create_h2_client() -> Client {
    Client::builder()
        .http2_prior_knowledge()
        .pool_max_idle_per_host(0)
        .build()
        .expect("Failed to create HTTP/2 client")
}

fn random_data(size_mb: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..size_mb * 1024 * 1024).map(|_| rng.r#gen()).collect()
}

async fn put_object(client: &Client, bucket: &str, key: &str, data: Vec<u8>) -> Result<(), String> {
    let url = format!("{}/{}/{}", PROXY_URL, bucket, key);
    let response = client
        .put(&url)
        .header("content-type", "application/octet-stream")
        .body(data)
        .send()
        .await
        .map_err(|e| format!("PUT failed: {}", e))?;

    if response.status().is_success() {
        Ok(())
    } else {
        Err(format!("PUT failed with status: {}", response.status()))
    }
}

async fn get_object(client: &Client, bucket: &str, key: &str) -> Result<usize, String> {
    let url = format!("{}/{}/{}", PROXY_URL, bucket, key);
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("GET failed: {}", e))?;

    if !response.status().is_success() {
        return Err(format!("GET failed with status: {}", response.status()));
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|e| format!("Failed to read body: {}", e))?;

    Ok(bytes.len())
}

async fn list_objects(client: &Client, bucket: &str, prefix: &str) -> Result<usize, String> {
    let url = format!("{}/{}?list-type=2&prefix={}", PROXY_URL, bucket, prefix);
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("LIST failed: {}", e))?;

    if !response.status().is_success() {
        return Err(format!("LIST failed with status: {}", response.status()));
    }

    // Just verify we got XML back
    let body = response
        .text()
        .await
        .map_err(|e| format!("Failed to read body: {}", e))?;

    if !body.contains("<ListBucketResult") {
        return Err(format!(
            "LIST returned invalid response: {}",
            &body[..100.min(body.len())]
        ));
    }

    Ok(1)
}

async fn delete_object(client: &Client, bucket: &str, key: &str) -> Result<(), String> {
    let url = format!("{}/{}/{}", PROXY_URL, bucket, key);
    client
        .delete(&url)
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("DELETE failed: {}", e))
}

/// Simulate a single L0 compaction burst: 5 concurrent SST uploads over HTTP/2
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

/// Simulate mixed workload: uploads + reads + lists over HTTP/2
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

    println!("\n=== ZeroFS Workload Simulation (HTTP/2) ===");
    println!("Bucket: {}", bucket);
    println!("SST size: 4MB");
    println!("Pattern: 5 concurrent uploads per burst");
    println!("Protocol: HTTP/2 with prior knowledge");
    println!();

    let client = create_h2_client();

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
            println!("SUCCESS: Proxy handled zerofs workload pattern over HTTP/2");
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

    println!("\n=== Large Concurrent Upload Test (HTTP/2) ===");
    println!("Bucket: {}", bucket);
    println!("File size: 8MB each");
    println!("Concurrent: 5 files");
    println!("Protocol: HTTP/2 with prior knowledge");
    println!();

    let client = create_h2_client();

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

/// Test range requests
#[tokio::test]
async fn test_range_requests() {
    let bucket = match std::env::var("BUNNY_STORAGE_ZONE") {
        Ok(b) => b,
        Err(_) => {
            eprintln!("Skipping: BUNNY_STORAGE_ZONE not set");
            return;
        }
    };

    println!("\n=== Range Request Test ===");
    let client = create_h2_client();
    let key = "zerofs-test/range-test.bin";
    let file_size = 1024 * 1024;
    let data: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();

    println!("Uploading 1MB test file...");
    put_object(&client, &bucket, key, data.clone())
        .await
        .expect("Upload failed");

    let url = format!("{}/{}/{}", PROXY_URL, bucket, key);

    // bytes=0-99
    println!("Test 1: bytes=0-99");
    let response = client
        .get(&url)
        .header("Range", "bytes=0-99")
        .send()
        .await
        .expect("GET failed");
    assert_eq!(response.status().as_u16(), 206);
    let content_range = response
        .headers()
        .get("content-range")
        .expect("Missing Content-Range")
        .to_str()
        .unwrap()
        .to_string();
    assert!(content_range.starts_with("bytes 0-99/"));
    let bytes = response.bytes().await.unwrap();
    assert_eq!(bytes.len(), 100);
    assert_eq!(&bytes[..], &data[0..100]);
    println!("  OK: {}", content_range);

    // bytes=1048476- (last 100)
    println!("Test 2: bytes=1048476-");
    let response = client
        .get(&url)
        .header("Range", "bytes=1048476-")
        .send()
        .await
        .expect("GET failed");
    assert_eq!(response.status().as_u16(), 206);
    let bytes = response.bytes().await.unwrap();
    assert_eq!(bytes.len(), 100);
    assert_eq!(&bytes[..], &data[1048476..]);
    println!("  OK");

    // bytes=-100 (suffix)
    println!("Test 3: bytes=-100");
    let response = client
        .get(&url)
        .header("Range", "bytes=-100")
        .send()
        .await
        .expect("GET failed");
    assert_eq!(response.status().as_u16(), 206);
    let bytes = response.bytes().await.unwrap();
    assert_eq!(bytes.len(), 100);
    assert_eq!(&bytes[..], &data[file_size - 100..]);
    println!("  OK");

    // bytes=500000-500099 (middle)
    println!("Test 4: bytes=500000-500099");
    let response = client
        .get(&url)
        .header("Range", "bytes=500000-500099")
        .send()
        .await
        .expect("GET failed");
    assert_eq!(response.status().as_u16(), 206);
    let bytes = response.bytes().await.unwrap();
    assert_eq!(bytes.len(), 100);
    assert_eq!(&bytes[..], &data[500000..500100]);
    println!("  OK");

    // 4KB block
    println!("Test 5: 4KB block");
    let response = client
        .get(&url)
        .header("Range", "bytes=0-4095")
        .send()
        .await
        .expect("GET failed");
    assert_eq!(response.status().as_u16(), 206);
    let bytes = response.bytes().await.unwrap();
    assert_eq!(bytes.len(), 4096);
    assert_eq!(&bytes[..], &data[0..4096]);
    println!("  OK");

    let _ = delete_object(&client, &bucket, key).await;
    println!("SUCCESS");
}

/// Test concurrent range requests
#[tokio::test]
async fn test_concurrent_range_requests() {
    let bucket = match std::env::var("BUNNY_STORAGE_ZONE") {
        Ok(b) => b,
        Err(_) => {
            eprintln!("Skipping: BUNNY_STORAGE_ZONE not set");
            return;
        }
    };

    println!("\n=== Concurrent Range Request Test ===");
    let client = create_h2_client();
    let key = "zerofs-test/concurrent-range-test.bin";
    let file_size = 4 * 1024 * 1024;
    let data: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();

    println!("Uploading 4MB test file...");
    put_object(&client, &bucket, key, data.clone())
        .await
        .expect("Upload failed");

    println!("Issuing 10 concurrent 4KB range requests...");
    let start = Instant::now();

    let ranges: Vec<_> = (0..10)
        .map(|i| {
            let offset = i * 400 * 1024;
            let client = client.clone();
            let url = format!("{}/{}/{}", PROXY_URL, bucket, key);
            let data = data.clone();
            async move {
                let range = format!("bytes={}-{}", offset, offset + 4095);
                let response = client
                    .get(&url)
                    .header("Range", &range)
                    .send()
                    .await
                    .map_err(|e| format!("GET failed: {}", e))?;

                if response.status().as_u16() != 206 {
                    return Err(format!("Expected 206, got {}", response.status()));
                }

                let bytes = response
                    .bytes()
                    .await
                    .map_err(|e| format!("Read failed: {}", e))?;

                if bytes.len() != 4096 {
                    return Err(format!("Expected 4096, got {}", bytes.len()));
                }

                let expected = &data[offset..offset + 4096];
                if &bytes[..] != expected {
                    return Err("Content mismatch".to_string());
                }

                Ok(offset)
            }
        })
        .collect();

    let results = join_all(ranges).await;
    let elapsed = start.elapsed();

    let mut success = 0;
    for result in &results {
        match result {
            Ok(offset) => {
                println!("  Offset {}: OK", offset);
                success += 1;
            }
            Err(e) => println!("  FAILED: {}", e),
        }
    }

    println!(
        "Duration: {:.3}s, {}/10 succeeded",
        elapsed.as_secs_f64(),
        success
    );
    let _ = delete_object(&client, &bucket, key).await;
    assert_eq!(success, 10);
    println!("SUCCESS");
}
