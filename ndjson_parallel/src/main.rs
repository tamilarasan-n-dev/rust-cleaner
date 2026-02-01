// Add to Cargo.toml:
// arrow = "53"
// parquet = "53"

use arrow::array::{ArrayRef, StringArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use flate2::read::GzDecoder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use serde_json::Value;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    sync::Arc,
    time::Instant,
};

const CHUNK_SIZE: usize = 50_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = "/media/tamil-07/New Volume1/torrents/gz/part-00001.gz";
    let output = "part-00001.parquet";

    println!("🚀 Using {} CPU cores", rayon::current_num_threads());
    println!("📖 Reading and parsing...\n");

    let start = Instant::now();

    // Read entire file into memory (300MB is fine)
    let file = File::open(input)?;
    let decoder = GzDecoder::new(file);
    let reader = BufReader::with_capacity(16 * 1024 * 1024, decoder);
    
    let lines: Vec<String> = reader.lines().flatten().collect();
    println!("✅ Read {} lines in {:.2}s", lines.len(), start.elapsed().as_secs_f64());

    // Parse in parallel using all cores
    let parse_start = Instant::now();
    let records: Vec<_> = lines
        .par_chunks(CHUNK_SIZE)
        .flat_map(|chunk| {
            chunk.par_iter().filter_map(|line| parse_json(line))
        })
        .collect();

    println!("✅ Parsed {} records in {:.2}s", records.len(), parse_start.elapsed().as_secs_f64());

    // Define schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("full_name", DataType::Utf8, true),
        Field::new("gender", DataType::Utf8, true),
        Field::new("job_title", DataType::Utf8, true),
        Field::new("location_country", DataType::Utf8, true),
        Field::new("location_region", DataType::Utf8, true),
        Field::new("location_continent", DataType::Utf8, true),
        Field::new("job_last_updated", DataType::Utf8, true),
        Field::new("experience", DataType::Utf8, true),
        Field::new("education", DataType::Utf8, true),
        Field::new("profiles", DataType::Utf8, true),
        Field::new("version_status", DataType::Utf8, true),
    ]));

    // Write to Parquet
    let write_start = Instant::now();
    let file = File::create(output)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(3)?,
        ))
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    // Write in batches
    for batch_records in records.chunks(100_000) {
        let batch = create_record_batch(&schema, batch_records)?;
        writer.write(&batch)?;
    }

    writer.close()?;
    println!("✅ Wrote Parquet in {:.2}s", write_start.elapsed().as_secs_f64());

    let total = start.elapsed().as_secs_f64();
    println!("\n🎉 Total time: {:.2}s ({:.0} rows/sec)", total, records.len() as f64 / total);

    Ok(())
}

fn parse_json(line: &str) -> Option<ParsedRecord> {
    let obj: serde_json::Map<String, Value> = serde_json::from_str(line).ok()?;
    
    Some(ParsedRecord {
        id: obj.get("id").and_then(|v| v.as_str()).map(String::from),
        full_name: obj.get("full_name").and_then(|v| v.as_str()).map(String::from),
        gender: obj.get("gender").and_then(|v| v.as_str()).map(String::from),
        job_title: obj.get("job_title").and_then(|v| v.as_str()).map(String::from),
        location_country: obj.get("location_country").and_then(|v| v.as_str()).map(String::from),
        location_region: obj.get("location_region").and_then(|v| v.as_str()).map(String::from),
        location_continent: obj.get("location_continent").and_then(|v| v.as_str()).map(String::from),
        job_last_updated: obj.get("job_last_updated").and_then(|v| v.as_str()).map(String::from),
        experience: obj.get("experience").map(|v| v.to_string()),
        education: obj.get("education").map(|v| v.to_string()),
        profiles: obj.get("profiles").map(|v| v.to_string()),
        version_status: obj.get("version_status").map(|v| v.to_string()),
    })
}

#[derive(Debug)]
struct ParsedRecord {
    id: Option<String>,
    full_name: Option<String>,
    gender: Option<String>,
    job_title: Option<String>,
    location_country: Option<String>,
    location_region: Option<String>,
    location_continent: Option<String>,
    job_last_updated: Option<String>,
    experience: Option<String>,
    education: Option<String>,
    profiles: Option<String>,
    version_status: Option<String>,
}

fn create_record_batch(
    schema: &Arc<Schema>,
    records: &[ParsedRecord],
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let id: ArrayRef = Arc::new(StringArray::from(
        records.iter().map(|r| r.id.as_deref()).collect::<Vec<_>>()
    ));
    let full_name: ArrayRef = Arc::new(StringArray::from(
        records.iter().map(|r| r.full_name.as_deref()).collect::<Vec<_>>()
    ));
    let gender: ArrayRef = Arc::new(StringArray::from(
        records.iter().map(|r| r.gender.as_deref()).collect::<Vec<_>>()
    ));
    let job_title: ArrayRef = Arc::new(StringArray::from(
        records.iter().map(|r| r.job_title.as_deref()).collect::<Vec<_>>()
    ));
    let location_country: ArrayRef = Arc::new(StringArray::from(
        records.iter().map(|r| r.location_country.as_deref()).collect::<Vec<_>>()
    ));
    let location_region: ArrayRef = Arc::new(StringArray::from(
        records.iter().map(|r| r.location_region.as_deref()).collect::<Vec<_>>()
    ));
    let location_continent: ArrayRef = Arc::new(StringArray::from(
        records.iter().map(|r| r.location_continent.as_deref()).collect::<Vec<_>>()
    ));
    let job_last_updated: ArrayRef = Arc::new(StringArray::from(
        records.iter().map(|r| r.job_last_updated.as_deref()).collect::<Vec<_>>()
    ));
    let experience: ArrayRef = Arc::new(StringArray::from(
        records.iter().map(|r| r.experience.as_deref()).collect::<Vec<_>>()
    ));
    let education: ArrayRef = Arc::new(StringArray::from(
        records.iter().map(|r| r.education.as_deref()).collect::<Vec<_>>()
    ));
    let profiles: ArrayRef = Arc::new(StringArray::from(
        records.iter().map(|r| r.profiles.as_deref()).collect::<Vec<_>>()
    ));
    let version_status: ArrayRef = Arc::new(StringArray::from(
        records.iter().map(|r| r.version_status.as_deref()).collect::<Vec<_>>()
    ));

    Ok(RecordBatch::try_new(
        schema.clone(),
        vec![
            id,
            full_name,
            gender,
            job_title,
            location_country,
            location_region,
            location_continent,
            job_last_updated,
            experience,
            education,
            profiles,
            version_status,
        ],
    )?)
}