//! GZ Cleaner - Parallel null/empty field removal from gz-compressed JSON files
//!
//! This program processes multiple .gz files in parallel using a worker pool.
//! Each worker:
//! 1. Reads a single gz file
//! 2. Removes all null and empty fields from each JSON object
//! 3. Compresses the cleaned data back to gz
//! 4. Writes to the output folder (gz_cleaned)

use crossbeam_channel::{bounded, Sender, Receiver};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::{Map, Value};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::thread;
use std::time::Instant;

const NUM_WORKERS: usize = 8;

/// Represents a file processing task
struct FileTask {
    input_path: String,
    output_path: String,
}

/// Statistics for a processed file
struct FileResult {
    file_name: String,
    rows_processed: u64,
    fields_removed: u64,
    duration_secs: f64,
    success: bool,
    error_msg: Option<String>,
}

/// Recursively removes null and empty fields from a JSON value
fn remove_null_empty(value: Value) -> Option<Value> {
    match value {
        Value::Null => None,
        Value::Array(arr) => {
            let cleaned: Vec<Value> = arr
                .into_iter()
                .filter_map(remove_null_empty)
                .collect();
            if cleaned.is_empty() {
                None
            } else {
                Some(Value::Array(cleaned))
            }
        }
        Value::Object(obj) => {
            let cleaned: Map<String, Value> = obj
                .into_iter()
                .filter_map(|(k, v)| {
                    remove_null_empty(v).map(|cleaned_v| (k, cleaned_v))
                })
                .collect();
            if cleaned.is_empty() {
                None
            } else {
                Some(Value::Object(cleaned))
            }
        }
        Value::String(s) => {
            if s.is_empty() {
                None
            } else {
                Some(Value::String(s))
            }
        }
        other => Some(other),
    }
}

/// Count the number of fields that would be removed
fn count_null_empty_fields(value: &Value) -> u64 {
    match value {
        Value::Null => 1,
        Value::Array(arr) => {
            let empty_count: u64 = arr.iter().map(count_null_empty_fields).sum();
            if arr.is_empty() { 1 } else { empty_count }
        }
        Value::Object(obj) => {
            let mut count = 0;
            for (_, v) in obj {
                if v.is_null() {
                    count += 1;
                } else if let Value::String(s) = v {
                    if s.is_empty() {
                        count += 1;
                    }
                } else if let Value::Array(a) = v {
                    if a.is_empty() {
                        count += 1;
                    } else {
                        count += count_null_empty_fields(v);
                    }
                } else if let Value::Object(o) = v {
                    if o.is_empty() {
                        count += 1;
                    } else {
                        count += count_null_empty_fields(v);
                    }
                }
            }
            count
        }
        Value::String(s) => if s.is_empty() { 1 } else { 0 },
        _ => 0,
    }
}

/// Process a single gz file: read, clean, compress, write
fn process_file(task: &FileTask) -> FileResult {
    let start = Instant::now();
    let file_name = Path::new(&task.input_path)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let mut rows_processed = 0u64;
    let mut fields_removed = 0u64;

    // Open input file
    let input_file = match File::open(&task.input_path) {
        Ok(f) => f,
        Err(e) => {
            return FileResult {
                file_name,
                rows_processed: 0,
                fields_removed: 0,
                duration_secs: start.elapsed().as_secs_f64(),
                success: false,
                error_msg: Some(format!("Failed to open input file: {}", e)),
            };
        }
    };

    // Create output file
    let output_file = match File::create(&task.output_path) {
        Ok(f) => f,
        Err(e) => {
            return FileResult {
                file_name,
                rows_processed: 0,
                fields_removed: 0,
                duration_secs: start.elapsed().as_secs_f64(),
                success: false,
                error_msg: Some(format!("Failed to create output file: {}", e)),
            };
        }
    };

    // Setup gz decoder and encoder
    let decoder = GzDecoder::new(input_file);
    let reader = BufReader::with_capacity(1024 * 1024, decoder); // 1MB buffer

    let encoder = GzEncoder::new(output_file, Compression::default());
    let mut writer = BufWriter::with_capacity(1024 * 1024, encoder); // 1MB buffer

    // Process line by line
    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                eprintln!("⚠️  Warning: Failed to read line in {}: {}", file_name, e);
                continue;
            }
        };

        // Skip empty lines
        if line.trim().is_empty() {
            continue;
        }

        // Parse JSON
        let value: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("⚠️  Warning: Failed to parse JSON in {}: {}", file_name, e);
                continue;
            }
        };

        // Count fields to be removed
        fields_removed += count_null_empty_fields(&value);

        // Clean the JSON
        if let Some(cleaned) = remove_null_empty(value) {
            // Serialize back to JSON string
            let json_str = match serde_json::to_string(&cleaned) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("⚠️  Warning: Failed to serialize JSON in {}: {}", file_name, e);
                    continue;
                }
            };

            // Write to output
            if let Err(e) = writeln!(writer, "{}", json_str) {
                eprintln!("⚠️  Warning: Failed to write line in {}: {}", file_name, e);
                continue;
            }
        }

        rows_processed += 1;

        // Progress indicator every 100k rows
        if rows_processed % 100_000 == 0 {
            println!("   📄 {} - Processed {} rows...", file_name, rows_processed);
        }
    }

    // Flush and finish compression
    if let Err(e) = writer.flush() {
        return FileResult {
            file_name,
            rows_processed,
            fields_removed,
            duration_secs: start.elapsed().as_secs_f64(),
            success: false,
            error_msg: Some(format!("Failed to flush writer: {}", e)),
        };
    }

    // Get the inner GzEncoder and finish it properly
    let encoder = match writer.into_inner() {
        Ok(e) => e,
        Err(e) => {
            return FileResult {
                file_name,
                rows_processed,
                fields_removed,
                duration_secs: start.elapsed().as_secs_f64(),
                success: false,
                error_msg: Some(format!("Failed to get encoder: {}", e)),
            };
        }
    };

    if let Err(e) = encoder.finish() {
        return FileResult {
            file_name,
            rows_processed,
            fields_removed,
            duration_secs: start.elapsed().as_secs_f64(),
            success: false,
            error_msg: Some(format!("Failed to finish compression: {}", e)),
        };
    }

    FileResult {
        file_name,
        rows_processed,
        fields_removed,
        duration_secs: start.elapsed().as_secs_f64(),
        success: true,
        error_msg: None,
    }
}

/// Worker function that processes files from the channel
fn worker(id: usize, receiver: Receiver<FileTask>, result_sender: Sender<FileResult>) {
    println!("🔧 Worker {} started", id);
    
    while let Ok(task) = receiver.recv() {
        println!("🚀 Worker {} processing: {}", id, task.input_path);
        let result = process_file(&task);
        
        if result.success {
            println!(
                "✅ Worker {} completed: {} ({} rows, {} fields removed, {:.2}s)",
                id, result.file_name, result.rows_processed, result.fields_removed, result.duration_secs
            );
        } else {
            println!(
                "❌ Worker {} failed: {} - {}",
                id,
                result.file_name,
                result.error_msg.as_ref().unwrap_or(&"Unknown error".to_string())
            );
        }
        
        let _ = result_sender.send(result);
    }
    
    println!("🔧 Worker {} finished", id);
}

fn main() {
    let total_start = Instant::now();
    
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║         GZ CLEANER - Parallel Null/Empty Field Remover         ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();

    // Input files to process
    let files = vec![
        "/media/tamil-07/1220581A2058075F/gz/gz/part-00000.gz",
        "/media/tamil-07/1220581A2058075F/gz/gz/part-00001.gz",
        // "/media/tamil-07/1220581A2058075F/gz/gz/part-00002.gz",
        "/media/tamil-07/1220581A2058075F/gz/gz/part-00003.gz",
        "/media/tamil-07/1220581A2058075F/gz/gz/part-00004.gz",
        "/media/tamil-07/1220581A2058075F/gz/gz/part-00005.gz",
    ];

    // Output directory
    let output_dir = "/media/tamil-07/1220581A2058075F/gz/gz_cleaned";

    // Create output directory if it doesn't exist
    if let Err(e) = fs::create_dir_all(output_dir) {
        eprintln!("❌ Failed to create output directory: {}", e);
        return;
    }

    println!("📁 Input files: {}", files.len());
    println!("📁 Output directory: {}", output_dir);
    println!("👷 Workers: {}", NUM_WORKERS);
    println!();

    // Create channels for task distribution and result collection
    let (task_sender, task_receiver) = bounded::<FileTask>(files.len());
    let (result_sender, result_receiver) = bounded::<FileResult>(files.len());

    // Spawn worker threads
    let mut handles = Vec::with_capacity(NUM_WORKERS);
    for id in 0..NUM_WORKERS {
        let receiver = task_receiver.clone();
        let sender = result_sender.clone();
        handles.push(thread::spawn(move || {
            worker(id, receiver, sender);
        }));
    }

    // Drop original receiver so workers can detect channel closure
    drop(task_receiver);
    drop(result_sender);

    // Send tasks to workers
    for input_path in &files {
        let file_name = Path::new(input_path)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        
        let output_path = format!("{}/{}", output_dir, file_name);
        
        let task = FileTask {
            input_path: input_path.to_string(),
            output_path,
        };
        
        if task_sender.send(task).is_err() {
            eprintln!("❌ Failed to send task for: {}", input_path);
        }
    }

    // Close the task channel to signal workers to finish
    drop(task_sender);

    // Collect results
    let mut total_rows = 0u64;
    let mut total_fields_removed = 0u64;
    let mut successful = 0usize;
    let mut failed = 0usize;

    for _ in 0..files.len() {
        if let Ok(result) = result_receiver.recv() {
            if result.success {
                successful += 1;
                total_rows += result.rows_processed;
                total_fields_removed += result.fields_removed;
            } else {
                failed += 1;
            }
        }
    }

    // Wait for all workers to finish
    for handle in handles {
        let _ = handle.join();
    }

    let total_duration = total_start.elapsed().as_secs_f64();

    // Print summary
    println!();
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                         FINAL SUMMARY                          ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!("📊 Files processed successfully: {}", successful);
    println!("❌ Files failed: {}", failed);
    println!("📝 Total rows processed: {}", total_rows);
    println!("🧹 Total null/empty fields removed: {}", total_fields_removed);
    println!("⏱️  Total time: {:.2}s", total_duration);
    println!("⚡ Throughput: {:.2} rows/sec", total_rows as f64 / total_duration);
    println!();
    println!("✨ Output written to: {}", output_dir);
}
