use flate2::read::GzDecoder;
use rayon::prelude::*;
use serde_json::Value;
use serde_json::to_writer_pretty;
use std::{
    collections::HashMap,
    collections::HashSet,
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write, Result},
    sync::LazyLock,
};


static ANALYTIC_FIELDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "gender",
        "location_country",
        "location_continent",
        "job_title",
        "version_status.status",
    ]
    .into_iter()
    .collect()
});

#[derive(Default, Clone)]
struct FieldStats {
    present: u64,
    null: u64,
    empty: u64,
    non_empty: u64,
}

#[derive(Default, Clone)]
struct FileStats {
    rows: u64,
    total_fields: u64,
    null_or_empty_fields: u64,
    per_field: HashMap<String, FieldStats>,
    value_counts: HashMap<String, HashMap<String, u32>>,
}

fn is_empty_value(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::Array(a) => a.is_empty(),
        Value::Object(o) => o.is_empty(),
        _ => false,
    }
}

fn write_value_distributions_json(
    path: &str,
    value_counts: &HashMap<String, HashMap<String, u32>>,
) -> Result<()> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);

    // Pretty JSON output
    to_writer_pretty(writer, value_counts)?;

    Ok(())
}

fn analyze_file_parallel(path: &str) -> FileStats {
    let file = File::open(path).unwrap();
    let decoder = GzDecoder::new(file);
    let reader = BufReader::new(decoder);

    reader
        .lines()
        .par_bridge()
        .filter_map(Result::ok)
        .fold(FileStats::default, |mut acc, line| {
            if let Ok(Value::Object(obj)) = serde_json::from_str::<Value>(&line) {
                acc.rows += 1;

                for (k, v) in obj {
                    acc.total_fields += 1;

                    // Per-field presence / null / empty stats
                    let entry = acc.per_field.entry(k.clone()).or_default();
                    entry.present += 1;

                    if is_empty_value(&v) {
                        acc.null_or_empty_fields += 1;
                        if v.is_null() {
                            entry.null += 1;
                        } else {
                            entry.empty += 1;
                        }
                    } else {
                        entry.non_empty += 1;
                    }

                    // Config-driven value counts
                    if ANALYTIC_FIELDS.contains(k.as_str()) {
                        if let Some(value) = v.as_str() {
                            let field_map = acc.value_counts.entry(k).or_insert_with(HashMap::new);

                            *field_map.entry(value.to_string()).or_insert(0) += 1;
                        }
                    }
                }
            }
            acc
        })
        .reduce(FileStats::default, |mut a, b| {
            a.rows += b.rows;
            a.total_fields += b.total_fields;
            a.null_or_empty_fields += b.null_or_empty_fields;

            // Merge per_field stats
            for (k, v) in b.per_field {
                let e = a.per_field.entry(k).or_default();
                e.present += v.present;
                e.null += v.null;
                e.empty += v.empty;
                e.non_empty += v.non_empty;
            }

            // Merge value_counts
            for (field, counts) in b.value_counts {
                let entry = a.value_counts.entry(field).or_insert_with(HashMap::new);
                for (val, count) in counts {
                    *entry.entry(val).or_insert(0) += count;
                }
            }

            a
        })
}

fn main() {
    let files = vec![
        "/media/tamil-07/1220581A2058075F/gz/gz/part-00000.gz",
        "/media/tamil-07/1220581A2058075F/gz/gz/part-00001.gz",
        // "/media/tamil-07/1220581A2058075F/gz/gz/part-00002.gz",
        "/media/tamil-07/1220581A2058075F/gz/gz/part-00003.gz",
        "/media/tamil-07/1220581A2058075F/gz/gz/part-00004.gz",
        "/media/tamil-07/1220581A2058075F/gz/gz/part-00005.gz",
        "/media/tamil-07/1220581A2058075F/gz/gz/part-00006.gz",
        "/media/tamil-07/1220581A2058075F/gz/gz/part-00007.gz",
        "/media/tamil-07/1220581A2058075F/gz/gz/part-00008.gz",
    ];

    let mut global = FileStats::default();

    for file in &files {
        println!("\n📂 Analyzing {file}");
        let stats = analyze_file_parallel(file);

        let avg_fields = stats.total_fields as f64 / stats.rows as f64;
        let avg_nulls = stats.null_or_empty_fields as f64 / stats.rows as f64;

        println!("Objects            : {}", stats.rows);
        println!("Avg fields/object  : {:.2}", avg_fields);
        println!("Avg null+empty/obj : {:.2}", avg_nulls);
        println!(
            "Null+empty ratio   : {:.2}%",
            (avg_nulls / avg_fields) * 100.0
        );

        global.rows += stats.rows;
        global.total_fields += stats.total_fields;
        global.null_or_empty_fields += stats.null_or_empty_fields;

        for (k, v) in stats.per_field {
            let e = global.per_field.entry(k).or_default();
            e.present += v.present;
            e.null += v.null;
            e.empty += v.empty;
            e.non_empty += v.non_empty;
        }
        for (field, counts) in stats.value_counts {
            let entry = global
                .value_counts
                .entry(field)
                .or_insert_with(HashMap::new);
            for (val, count) in counts {
                *entry.entry(val).or_insert(0) += count;
            }
        }
    }

    println!("\n📊 OVERALL SUMMARY ({} files)", files.len());
    let avg_fields = global.total_fields as f64 / global.rows as f64;
    let avg_nulls = global.null_or_empty_fields as f64 / global.rows as f64;

    println!("Total objects       : {}", global.rows);
    println!("Avg fields/object   : {:.2}", avg_fields);
    println!("Avg null+empty/obj  : {:.2}", avg_nulls);
    println!(
        "Overall null ratio  : {:.2}%",
        (avg_nulls / avg_fields) * 100.0
    );

    write_value_distributions_json("value_distributions.json", &global.value_counts).unwrap();
}
