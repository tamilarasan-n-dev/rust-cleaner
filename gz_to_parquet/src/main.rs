//! GZ to Parquet Converter - Parallel processing of gz files to Parquet format
//!
//! Architecture:
//! - 8 independent worker processes
//! - Each worker: reads 1 gz file → writes 1 parquet file
//! - Final output: directory of parquet files
//!
//! Flow:
//! 400 gz files → 8 workers → 400 parquet files

use crossbeam_channel::{bounded, Receiver, Sender};
use duckdb::{Connection, Result as DuckResult, Appender};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::thread;
use std::time::Instant;

const NUM_WORKERS: usize = 8;

/// Task for a worker to process
struct FileTask {
    input_path: String,
    output_path: String,
}

/// Result from processing a file
struct FileResult {
    file_name: String,
    rows_processed: u64,
    duration_secs: f64,
    success: bool,
    error_msg: Option<String>,
}

/// Create the DuckDB table with full schema
fn create_table(conn: &Connection) -> DuckResult<()> {
    conn.execute_batch(
        r#"
        PRAGMA threads=1;
        PRAGMA memory_limit='2GB';
        
        CREATE TABLE people (
            id TEXT,
            full_name TEXT,
            first_name TEXT,
            middle_initial TEXT,
            middle_name TEXT,
            last_name TEXT,
            gender TEXT,
            birth_year INTEGER,
            birth_date TEXT,
            linkedin_url TEXT,
            linkedin_username TEXT,
            linkedin_id TEXT,
            facebook_url TEXT,
            facebook_username TEXT,
            facebook_id TEXT,
            twitter_url TEXT,
            twitter_username TEXT,
            github_url TEXT,
            github_username TEXT,
            work_email TEXT,
            mobile_phone TEXT,
            industry TEXT,
            job_title TEXT,
            job_title_role TEXT,
            job_title_sub_role TEXT,
            job_title_levels TEXT,
            job_company_id TEXT,
            job_company_name TEXT,
            job_company_website TEXT,
            job_company_size TEXT,
            job_company_founded INTEGER,
            job_company_industry TEXT,
            job_company_linkedin_url TEXT,
            job_company_linkedin_id TEXT,
            job_company_facebook_url TEXT,
            job_company_twitter_url TEXT,
            job_company_location_name TEXT,
            job_company_location_locality TEXT,
            job_company_location_metro TEXT,
            job_company_location_region TEXT,
            job_company_location_geo TEXT,
            job_company_location_street_address TEXT,
            job_company_location_address_line_2 TEXT,
            job_company_location_postal_code TEXT,
            job_company_location_country TEXT,
            job_company_location_continent TEXT,
            job_last_updated TEXT,
            job_start_date TEXT,
            job_summary TEXT,
            location_name TEXT,
            location_locality TEXT,
            location_metro TEXT,
            location_region TEXT,
            location_country TEXT,
            location_continent TEXT,
            location_street_address TEXT,
            location_address_line_2 TEXT,
            location_postal_code TEXT,
            location_geo TEXT,
            location_last_updated TEXT,
            linkedin_connections INTEGER,
            inferred_salary TEXT,
            inferred_years_experience INTEGER,
            summary TEXT,
            phone_numbers TEXT,
            emails TEXT,
            interests TEXT,
            skills TEXT,
            location_names TEXT,
            regions TEXT,
            countries TEXT,
            street_addresses TEXT,
            experience TEXT,
            education TEXT,
            profiles TEXT,
            certifications TEXT,
            languages TEXT,
            version_status TEXT
        );
        "#,
    )
}

/// Parse a JSON line and append to DuckDB using fast Appender API
#[inline]
fn parse_and_append(line: &str, appender: &mut Appender) -> Result<(), Box<dyn std::error::Error>> {
    let obj: Value = serde_json::from_str(line)?;
    let obj = match obj.as_object() {
        Some(o) => o,
        None => return Ok(()),
    };

    // Helper macros for extracting values
    macro_rules! get_str {
        ($field:expr) => {
            obj.get($field).and_then(|v| v.as_str()).map(String::from)
        };
    }
    
    macro_rules! get_i32 {
        ($field:expr) => {
            obj.get($field).and_then(|v| v.as_i64()).map(|n| n as i32)
        };
    }
    
    macro_rules! get_json {
        ($field:expr) => {
            obj.get($field).map(|v| v.to_string())
        };
    }

    appender.append_row([
        get_str!("id"),
        get_str!("full_name"),
        get_str!("first_name"),
        get_str!("middle_initial"),
        get_str!("middle_name"),
        get_str!("last_name"),
        get_str!("gender"),
    ])?;

    // Since DuckDB Appender doesn't support mixed types easily,
    // let's use a prepared statement approach instead
    Ok(())
}

/// Process a single gz file and write to parquet
fn process_file(task: &FileTask) -> FileResult {
    let start = Instant::now();
    let file_name = Path::new(&task.input_path)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    // Open input file
    let input_file = match File::open(&task.input_path) {
        Ok(f) => f,
        Err(e) => {
            return FileResult {
                file_name,
                rows_processed: 0,
                duration_secs: start.elapsed().as_secs_f64(),
                success: false,
                error_msg: Some(format!("Failed to open input file: {}", e)),
            };
        }
    };

    // Create DuckDB in-memory connection
    let conn = match Connection::open_in_memory() {
        Ok(c) => c,
        Err(e) => {
            return FileResult {
                file_name,
                rows_processed: 0,
                duration_secs: start.elapsed().as_secs_f64(),
                success: false,
                error_msg: Some(format!("Failed to create DuckDB connection: {}", e)),
            };
        }
    };

    // Create table
    if let Err(e) = create_table(&conn) {
        return FileResult {
            file_name,
            rows_processed: 0,
            duration_secs: start.elapsed().as_secs_f64(),
            success: false,
            error_msg: Some(format!("Failed to create table: {}", e)),
        };
    }

    // Prepare statement with 78 columns
    let sql = "INSERT INTO people VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(e) => {
            return FileResult {
                file_name,
                rows_processed: 0,
                duration_secs: start.elapsed().as_secs_f64(),
                success: false,
                error_msg: Some(format!("Failed to prepare statement: {}", e)),
            };
        }
    };

    // Setup gz decoder
    let decoder = GzDecoder::new(input_file);
    let reader = BufReader::with_capacity(8 * 1024 * 1024, decoder); // 8MB buffer

    let mut rows_processed = 0u64;

    // Process line by line
    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => continue,
        };

        // Skip empty lines
        if line.trim().is_empty() {
            continue;
        }

        // Parse JSON
        let obj: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        
        let obj = match obj.as_object() {
            Some(o) => o,
            None => continue,
        };

        // Extract all fields
        let id = obj.get("id").and_then(|v| v.as_str());
        let full_name = obj.get("full_name").and_then(|v| v.as_str());
        let first_name = obj.get("first_name").and_then(|v| v.as_str());
        let middle_initial = obj.get("middle_initial").and_then(|v| v.as_str());
        let middle_name = obj.get("middle_name").and_then(|v| v.as_str());
        let last_name = obj.get("last_name").and_then(|v| v.as_str());
        let gender = obj.get("gender").and_then(|v| v.as_str());
        let birth_year = obj.get("birth_year").and_then(|v| v.as_i64()).map(|n| n as i32);
        let birth_date = obj.get("birth_date").and_then(|v| v.as_str());
        let linkedin_url = obj.get("linkedin_url").and_then(|v| v.as_str());
        let linkedin_username = obj.get("linkedin_username").and_then(|v| v.as_str());
        let linkedin_id = obj.get("linkedin_id").and_then(|v| v.as_str());
        let facebook_url = obj.get("facebook_url").and_then(|v| v.as_str());
        let facebook_username = obj.get("facebook_username").and_then(|v| v.as_str());
        let facebook_id = obj.get("facebook_id").and_then(|v| v.as_str());
        let twitter_url = obj.get("twitter_url").and_then(|v| v.as_str());
        let twitter_username = obj.get("twitter_username").and_then(|v| v.as_str());
        let github_url = obj.get("github_url").and_then(|v| v.as_str());
        let github_username = obj.get("github_username").and_then(|v| v.as_str());
        let work_email = obj.get("work_email").and_then(|v| v.as_str());
        let mobile_phone = obj.get("mobile_phone").and_then(|v| v.as_str());
        let industry = obj.get("industry").and_then(|v| v.as_str());
        let job_title = obj.get("job_title").and_then(|v| v.as_str());
        let job_title_role = obj.get("job_title_role").and_then(|v| v.as_str());
        let job_title_sub_role = obj.get("job_title_sub_role").and_then(|v| v.as_str());
        let job_title_levels = obj.get("job_title_levels").map(|v| v.to_string());
        let job_company_id = obj.get("job_company_id").and_then(|v| v.as_str());
        let job_company_name = obj.get("job_company_name").and_then(|v| v.as_str());
        let job_company_website = obj.get("job_company_website").and_then(|v| v.as_str());
        let job_company_size = obj.get("job_company_size").and_then(|v| v.as_str());
        let job_company_founded = obj.get("job_company_founded").and_then(|v| v.as_i64()).map(|n| n as i32);
        let job_company_industry = obj.get("job_company_industry").and_then(|v| v.as_str());
        let job_company_linkedin_url = obj.get("job_company_linkedin_url").and_then(|v| v.as_str());
        let job_company_linkedin_id = obj.get("job_company_linkedin_id").and_then(|v| v.as_str());
        let job_company_facebook_url = obj.get("job_company_facebook_url").and_then(|v| v.as_str());
        let job_company_twitter_url = obj.get("job_company_twitter_url").and_then(|v| v.as_str());
        let job_company_location_name = obj.get("job_company_location_name").and_then(|v| v.as_str());
        let job_company_location_locality = obj.get("job_company_location_locality").and_then(|v| v.as_str());
        let job_company_location_metro = obj.get("job_company_location_metro").and_then(|v| v.as_str());
        let job_company_location_region = obj.get("job_company_location_region").and_then(|v| v.as_str());
        let job_company_location_geo = obj.get("job_company_location_geo").and_then(|v| v.as_str());
        let job_company_location_street_address = obj.get("job_company_location_street_address").and_then(|v| v.as_str());
        let job_company_location_address_line_2 = obj.get("job_company_location_address_line_2").and_then(|v| v.as_str());
        let job_company_location_postal_code = obj.get("job_company_location_postal_code").and_then(|v| v.as_str());
        let job_company_location_country = obj.get("job_company_location_country").and_then(|v| v.as_str());
        let job_company_location_continent = obj.get("job_company_location_continent").and_then(|v| v.as_str());
        let job_last_updated = obj.get("job_last_updated").and_then(|v| v.as_str());
        let job_start_date = obj.get("job_start_date").and_then(|v| v.as_str());
        let job_summary = obj.get("job_summary").and_then(|v| v.as_str());
        let location_name = obj.get("location_name").and_then(|v| v.as_str());
        let location_locality = obj.get("location_locality").and_then(|v| v.as_str());
        let location_metro = obj.get("location_metro").and_then(|v| v.as_str());
        let location_region = obj.get("location_region").and_then(|v| v.as_str());
        let location_country = obj.get("location_country").and_then(|v| v.as_str());
        let location_continent = obj.get("location_continent").and_then(|v| v.as_str());
        let location_street_address = obj.get("location_street_address").and_then(|v| v.as_str());
        let location_address_line_2 = obj.get("location_address_line_2").and_then(|v| v.as_str());
        let location_postal_code = obj.get("location_postal_code").and_then(|v| v.as_str());
        let location_geo = obj.get("location_geo").and_then(|v| v.as_str());
        let location_last_updated = obj.get("location_last_updated").and_then(|v| v.as_str());
        let linkedin_connections = obj.get("linkedin_connections").and_then(|v| v.as_i64()).map(|n| n as i32);
        let inferred_salary = obj.get("inferred_salary").and_then(|v| v.as_str());
        let inferred_years_experience = obj.get("inferred_years_experience").and_then(|v| v.as_i64()).map(|n| n as i32);
        let summary = obj.get("summary").and_then(|v| v.as_str());
        let phone_numbers = obj.get("phone_numbers").map(|v| v.to_string());
        let emails = obj.get("emails").map(|v| v.to_string());
        let interests = obj.get("interests").map(|v| v.to_string());
        let skills = obj.get("skills").map(|v| v.to_string());
        let location_names = obj.get("location_names").map(|v| v.to_string());
        let regions = obj.get("regions").map(|v| v.to_string());
        let countries = obj.get("countries").map(|v| v.to_string());
        let street_addresses = obj.get("street_addresses").map(|v| v.to_string());
        let experience = obj.get("experience").map(|v| v.to_string());
        let education = obj.get("education").map(|v| v.to_string());
        let profiles = obj.get("profiles").map(|v| v.to_string());
        let certifications = obj.get("certifications").map(|v| v.to_string());
        let languages = obj.get("languages").map(|v| v.to_string());
        let version_status = obj.get("version_status").map(|v| v.to_string());

        // Execute prepared statement
        if stmt.execute(duckdb::params![
            id, full_name, first_name, middle_initial, middle_name, last_name, gender,
            birth_year, birth_date, linkedin_url, linkedin_username, linkedin_id,
            facebook_url, facebook_username, facebook_id, twitter_url, twitter_username,
            github_url, github_username, work_email, mobile_phone, industry, job_title,
            job_title_role, job_title_sub_role, job_title_levels, job_company_id,
            job_company_name, job_company_website, job_company_size, job_company_founded,
            job_company_industry, job_company_linkedin_url, job_company_linkedin_id,
            job_company_facebook_url, job_company_twitter_url, job_company_location_name,
            job_company_location_locality, job_company_location_metro, job_company_location_region,
            job_company_location_geo, job_company_location_street_address,
            job_company_location_address_line_2, job_company_location_postal_code,
            job_company_location_country, job_company_location_continent, job_last_updated,
            job_start_date, job_summary, location_name, location_locality, location_metro,
            location_region, location_country, location_continent, location_street_address,
            location_address_line_2, location_postal_code, location_geo, location_last_updated,
            linkedin_connections, inferred_salary, inferred_years_experience, summary,
            phone_numbers, emails, interests, skills, location_names, regions, countries,
            street_addresses, experience, education, profiles, certifications, languages,
            version_status
        ]).is_ok() {
            rows_processed += 1;
        }

        // Progress indicator every 100k rows
        if rows_processed % 100_000 == 0 && rows_processed > 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let rate = rows_processed as f64 / elapsed;
            println!("   📄 {} - {} rows ({:.0} rows/sec)", file_name, rows_processed, rate);
        }
    }

    // Drop statement before using conn again
    drop(stmt);

    // Write to Parquet
    let parquet_sql = format!(
        "COPY people TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD);",
        task.output_path
    );

    if let Err(e) = conn.execute(&parquet_sql, []) {
        return FileResult {
            file_name,
            rows_processed,
            duration_secs: start.elapsed().as_secs_f64(),
            success: false,
            error_msg: Some(format!("Failed to write Parquet: {}", e)),
        };
    }

    FileResult {
        file_name,
        rows_processed,
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
                "✅ Worker {} completed: {} ({} rows, {:.2}s, {:.0} rows/sec)",
                id, result.file_name, result.rows_processed, result.duration_secs,
                result.rows_processed as f64 / result.duration_secs
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
    println!("║       GZ TO PARQUET - Parallel Column-Oriented Converter       ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();

    // Input files to process (add your 400 files here or use glob)
    let files = vec![
        "/media/tamil-07/1220581A2058075F/gz/gz/part-00000.gz",
        // "/media/tamil-07/1220581A2058075F/gz/gz/part-00001.gz",
        // "/media/tamil-07/1220581A2058075F/gz/gz/part-00002.gz",
        // "/media/tamil-07/1220581A2058075F/gz/gz/part-00003.gz",
        // "/media/tamil-07/1220581A2058075F/gz/gz/part-00004.gz",
        // "/media/tamil-07/1220581A2058075F/gz/gz/part-00005.gz",
    ];

    // Output directory
    let output_dir = "/media/tamil-07/1220581A2058075F/gz/parquet_output";

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
            .replace(".gz", ".parquet");

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
    let mut successful = 0usize;
    let mut failed = 0usize;

    for _ in 0..files.len() {
        if let Ok(result) = result_receiver.recv() {
            if result.success {
                successful += 1;
                total_rows += result.rows_processed;
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
    println!("⏱️  Total time: {:.2}s", total_duration);
    println!("⚡ Throughput: {:.2} rows/sec", total_rows as f64 / total_duration);
    println!();
    println!("📦 Parquet files written to: {}", output_dir);
    println!("   Each file is a column-oriented, ZSTD compressed Parquet file.");
}
