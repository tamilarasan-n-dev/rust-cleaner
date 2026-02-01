# Rust Cleaner 🦀

> Processing 1.5TB of NDJSON data on a potato laptop with Rust - A collection of high-performance parallel data processing tools.

This repository contains a suite of Rust tools designed to efficiently process large-scale NDJSON datasets. Born from processing ~300 million JSON objects (~1.5TB uncompressed) on an i3 laptop with 8GB RAM and a 5400 RPM HDD, these tools demonstrate that you don't need big hardware for big data processing.

## 🚀 Overview

This project showcases how to handle massive datasets using:
- **True parallelism** with Rust's Rayon (no GIL limitations)
- **Streaming I/O** without loading entire files into memory
- **Efficient compression/decompression** with gz files
- **Columnar storage** conversion for analytics workloads
- **Worker pool architectures** for CPU-bound tasks

## 🛠️ Tools

### 1. **null_analyser** - Statistical Analysis Engine
Parallel statistical analysis of large NDJSON datasets to understand data sparsity and field distributions.

**Features:**
- Calculates null/empty field ratios across millions of records
- Per-field presence statistics
- Configurable value distribution analysis for specific fields
- Uses `par_bridge()` for true parallel processing

**Usage:**
```bash
cd null_analyser
cargo run --release
```

**Sample Output:**
```
📂 Analyzing part-00000.gz
Objects            : 742500
Avg fields/object  : 78.00
Avg null+empty/obj : 50.4
Null+empty ratio   : 64.68%

📊 OVERALL SUMMARY (391 files)
Total objects       : 292576687
Overall null ratio  : 64.64%
```

### 2. **gz_cleaner** - Parallel Null Field Remover
High-performance parallel processor that removes null and empty fields from compressed NDJSON files.

**Features:**
- 8-worker thread pool architecture
- Streaming gz → clean → gz pipeline
- Removes null, empty arrays, empty objects, and empty strings
- 1MB buffered I/O for optimal performance

**Usage:**
```bash
cd gz_cleaner
cargo run --release
```

**Architecture:**
```
[GZ files] → 8 worker threads → [Cleaned GZ files]
```

### 3. **gz_to_parquet** - Columnar Storage Converter
Converts gz-compressed NDJSON files to Parquet format using DuckDB for optimal analytics performance.

**Features:**
- Parallel gz to Parquet conversion
- Uses DuckDB's embedded engine
- Optimized for sparse datasets
- ~200x compression improvement for null-heavy columns

**Usage:**
```bash
cd gz_to_parquet
cargo run --release
```

**Benefits:**
- **7GB → 37MB** for a single sparse column
- Orders of magnitude faster analytics queries
- SIMD-friendly columnar layout

### 4. **parquet_generator** - Advanced Parquet Pipeline
Enhanced Parquet generation with DuckDB integration and advanced schema handling.

**Usage:**
```bash
cd parquet_generator
cargo run --release
```

### 5. **ndjson_parallel** - Parallel NDJSON Processor
Specialized parallel processor for NDJSON data with email extraction and filtering capabilities.

**Features:**
- Email pattern matching and extraction
- Skills-based filtering
- Parallel processing with configurable workers

**Usage:**
```bash
cd ndjson_parallel
cargo run --release
```

## 📊 Performance Results

### Hardware Specs (The "Potato" Setup)
- **CPU**: Intel i3-1215U (2P + 4E cores, 8 threads)
- **RAM**: 8 GB DDR4 3200 MHz  
- **Disk**: 5400 RPM External HDD
- **OS**: Linux (Ubuntu)

### Dataset Processed
- **Files**: ~430 `.gz` files
- **Compressed size**: ~180 GB
- **Uncompressed size**: ~1.2–1.5 TB
- **Objects**: ~280–320 million JSON objects
- **Schema**: 78 fields per object, ~65% null/empty values

### Results
- **Analysis time**: ~3.7 hours for statistical analysis
- **Final Parquet size**: ~90 GB (from ~1.5 TB)
- **Data loss**: 0%
- **Compression ratio**: ~200x for sparse columns

## 🏗️ Architecture Patterns

### Parallel Processing with Rayon
```rust
reader
    .lines()
    .par_bridge()
    .filter_map(Result::ok)
    .fold(FileStats::default, |mut acc, line| {
        // Process each line in parallel
        acc
    })
    .reduce(FileStats::default, |a, b| a.merge(b));
```

### Worker Pool Pattern
```rust
// 8 workers processing files from a channel
let (sender, receiver) = bounded(NUM_WORKERS * 2);

for i in 0..NUM_WORKERS {
    let receiver = receiver.clone();
    thread::spawn(move || worker(i, receiver));
}
```

### Streaming I/O
```rust
let decoder = GzDecoder::new(input_file);
let reader = BufReader::with_capacity(1024 * 1024, decoder); // 1MB buffer
let encoder = GzEncoder::new(output_file, Compression::default());
let mut writer = BufWriter::with_capacity(1024 * 1024, encoder);
```

## 🚦 Quick Start

1. **Clone the repository:**
   ```bash
   git clone <your-repo-url>
   cd rust-cleaner
   ```

2. **Build all tools:**
   ```bash
   # Build individual tools
   cd null_analyser && cargo build --release
   cd ../gz_cleaner && cargo build --release
   cd ../gz_to_parquet && cargo build --release
   cd ../parquet_generator && cargo build --release
   cd ../ndjson_parallel && cargo build --release
   ```

3. **Prepare your data:**
   - Place your `.gz` files in the appropriate input directory
   - Update file paths in the source code to match your setup

4. **Run analysis:**
   ```bash
   cd null_analyser
   cargo run --release
   ```

5. **Convert to Parquet:**
   ```bash
   cd gz_to_parquet
   cargo run --release
   ```

## 📈 Key Learnings

1. **NDJSON is terrible at scale** - Row-oriented formats don't compress efficiently for sparse data
2. **gzip hides inefficiencies** - Dictionary-based compression masks the true cost of null fields
3. **Parallel I/O beats clever algorithms** - Utilizing all CPU cores is crucial for large datasets
4. **Columnar formats are not optional** - Parquet's compression and query performance are game-changing
5. **Rust enables "big data" without "big hardware"** - True parallelism and zero-cost abstractions matter

## 🔧 Dependencies

- **flate2**: GZ compression/decompression
- **serde_json**: JSON parsing and serialization
- **rayon**: Data parallelism
- **crossbeam-channel**: Worker communication
- **duckdb**: Embedded SQL engine for Parquet generation

## 📝 Configuration

Each tool can be configured by modifying the constants and file paths in the respective `main.rs` files:

- `NUM_WORKERS`: Number of parallel workers
- Input/output file paths
- Buffer sizes for I/O operations
- Field selections for analysis

## 🎯 Use Cases

- **Data Quality Assessment**: Analyze field sparsity and distributions
- **Data Cleaning**: Remove unnecessary null fields from large datasets  
- **Format Migration**: Convert row-oriented data to columnar formats
- **Storage Optimization**: Reduce storage costs through efficient compression
- **Analytics Preparation**: Prepare data for fast analytical queries

## 🤝 Contributing

Feel free to open issues or submit pull requests for improvements, bug fixes, or additional features.

## 📄 License

This project is open source. Please check individual files for specific licensing information.

---

*"This was not about being clever. It was about understanding formats, measuring before optimizing, respecting I/O limits, and choosing the right data layout."*