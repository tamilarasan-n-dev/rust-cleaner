use crossbeam_channel::{bounded, Receiver, Sender};
use duckdb::{params, Connection, Result};
use flate2::read::GzDecoder;
use rayon::prelude::*;
use serde_json::Value;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::Instant,
};

const CHUNK_SIZE: usize = 10_000; // Smaller chunks for better parallelism
const CHANNEL_BUFFER: usize = 16;

type Row = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn main() -> Result<()> {
    let input = "/media/tamil-07/New Volume1/torrents/gz/part-00001.gz";
    let output = "part-00001.parquet";

    let num_threads = rayon::current_num_threads();
    println!("🚀 Using {} CPU cores for parallel processing", num_threads);
    println!("📖 Starting pipelined processing...\n");

    let start_time = Instant::now();
    let total_rows = AtomicUsize::new(0);

    let (line_sender, line_receiver): (Sender<Vec<String>>, Receiver<Vec<String>>) =
        bounded(CHANNEL_BUFFER);
    let (row_sender, row_receiver): (Sender<Vec<Row>>, Receiver<Vec<Row>>) =
        bounded(CHANNEL_BUFFER);

    // ==================== READER THREAD ====================
    let reader_handle = thread::spawn(move || {
        let file = File::open(input).expect("Failed to open input file");
        let decoder = GzDecoder::new(file);
        let reader = BufReader::with_capacity(16 * 1024 * 1024, decoder);

        let mut chunk: Vec<String> = Vec::with_capacity(CHUNK_SIZE);
        let mut chunks_sent = 0;

        for line in reader.lines().flatten() {
            chunk.push(line);

            if chunk.len() >= CHUNK_SIZE {
                line_sender.send(chunk).expect("Failed to send chunk");
                chunk = Vec::with_capacity(CHUNK_SIZE);
                chunks_sent += 1;
            }
        }

        if !chunk.is_empty() {
            line_sender.send(chunk).expect("Failed to send final chunk");
            chunks_sent += 1;
        }

        drop(line_sender);
        chunks_sent
    });

    // ==================== PARSER THREADS (via rayon) ====================
    let parser_handle = thread::spawn(move || {
        let mut batches_sent = 0;

        for lines_chunk in line_receiver {
            let parsed_rows: Vec<Row> = lines_chunk
                .par_iter()
                .filter_map(|line| parse_json_line(line))
                .collect();

            if !parsed_rows.is_empty() {
                row_sender.send(parsed_rows).expect("Failed to send rows");
                batches_sent += 1;
            }
        }

        drop(row_sender);
        batches_sent
    });

    // ==================== WRITER THREAD (main) - OPTIMIZED ====================
    let mut conn = Connection::open_in_memory()?;

    // Optimize DuckDB for bulk loading
    conn.execute_batch(
        r#"
        PRAGMA threads=1;
        PRAGMA memory_limit='4GB';
        
        CREATE TABLE people (
            id TEXT,
            full_name TEXT,
            gender TEXT,
            job_title TEXT,
            location_country TEXT,
            location_region TEXT,
            location_continent TEXT,
            job_last_updated DATE,
            experience JSON,
            education JSON,
            profiles JSON,
            version_status JSON
        );
        "#,
    )?;

    let mut batch_num = 0;
    let mut last_report_time = Instant::now();
    let mut rows_since_last_report = 0;

    for parsed_batch in row_receiver {
        let batch_len = parsed_batch.len();
        total_rows.fetch_add(batch_len, Ordering::Relaxed);
        rows_since_last_report += batch_len;

        // Use optimized bulk insert
        insert_batch_optimized(&mut conn, &parsed_batch)?;

        batch_num += 1;

        let current_total = total_rows.load(Ordering::Relaxed);
        if rows_since_last_report >= 100_000 {
            let elapsed = last_report_time.elapsed();
            let rows_per_sec = rows_since_last_report as f64 / elapsed.as_secs_f64();
            
            println!(
                "  ✅ Processed {:>7} rows | Batch #{:>2} | ⏱️  {:.2}s | 🚀 {:.0} rows/sec",
                current_total,
                batch_num,
                elapsed.as_secs_f64(),
                rows_per_sec
            );
            
            last_report_time = Instant::now();
            rows_since_last_report = 0;
        }
    }

    let chunks_read = reader_handle.join().expect("Reader thread panicked");
    let batches_parsed = parser_handle.join().expect("Parser thread panicked");

    let final_count = total_rows.load(Ordering::Relaxed);
    let total_elapsed = start_time.elapsed();

    println!("\n📊 Processing complete:");
    println!("   Total rows: {}", final_count);
    println!("   Chunks read: {}", chunks_read);
    println!("   Batches parsed: {}", batches_parsed);
    println!("   Total time: {:.2}s", total_elapsed.as_secs_f64());
    println!(
        "   Throughput: {:.0} rows/sec",
        final_count as f64 / total_elapsed.as_secs_f64()
    );

    println!("\n💾 Writing Parquet file...");
    let parquet_start = Instant::now();
    conn.execute(
        &format!(
            "COPY people TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD);",
            output
        ),
        [],
    )?;
    println!(
        "✅ Parquet written: {} ({:.2}s)",
        output,
        parquet_start.elapsed().as_secs_f64()
    );

    Ok(())
}

#[inline]
fn parse_json_line(line: &str) -> Option<Row> {
    if let Ok(Value::Object(obj)) = serde_json::from_str::<Value>(line) {
        Some((
            obj.get("id").and_then(|v| v.as_str()).map(|s| s.to_string()),
            obj.get("full_name").and_then(|v| v.as_str()).map(|s| s.to_string()),
            obj.get("gender").and_then(|v| v.as_str()).map(|s| s.to_string()),
            obj.get("job_title").and_then(|v| v.as_str()).map(|s| s.to_string()),
            obj.get("location_country").and_then(|v| v.as_str()).map(|s| s.to_string()),
            obj.get("location_region").and_then(|v| v.as_str()).map(|s| s.to_string()),
            obj.get("location_continent").and_then(|v| v.as_str()).map(|s| s.to_string()),
            obj.get("job_last_updated").and_then(|v| v.as_str()).map(|s| s.to_string()),
            obj.get("experience").map(|v| v.to_string()),
            obj.get("education").map(|v| v.to_string()),
            obj.get("profiles").map(|v| v.to_string()),
            obj.get("version_status").map(|v| v.to_string()),
        ))
    } else {
        None
    }
}

/// Optimized bulk insert using VALUES clause
fn insert_batch_optimized(conn: &mut Connection, batch: &[Row]) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    // Build a single INSERT with multiple VALUES
    let placeholders = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    let values_clause = (0..batch.len())
        .map(|_| placeholders)
        .collect::<Vec<_>>()
        .join(", ");
    
    let sql = format!("INSERT INTO people VALUES {}", values_clause);
    
    // Flatten all parameters
    let mut all_params: Vec<&dyn duckdb::ToSql> = Vec::with_capacity(batch.len() * 12);
    
    for row in batch {
        all_params.push(&row.0 as &dyn duckdb::ToSql);
        all_params.push(&row.1 as &dyn duckdb::ToSql);
        all_params.push(&row.2 as &dyn duckdb::ToSql);
        all_params.push(&row.3 as &dyn duckdb::ToSql);
        all_params.push(&row.4 as &dyn duckdb::ToSql);
        all_params.push(&row.5 as &dyn duckdb::ToSql);
        all_params.push(&row.6 as &dyn duckdb::ToSql);
        all_params.push(&row.7 as &dyn duckdb::ToSql);
        all_params.push(&row.8 as &dyn duckdb::ToSql);
        all_params.push(&row.9 as &dyn duckdb::ToSql);
        all_params.push(&row.10 as &dyn duckdb::ToSql);
        all_params.push(&row.11 as &dyn duckdb::ToSql);
    }
    
    conn.execute(&sql, all_params.as_slice())?;
    Ok(())
}