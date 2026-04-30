//! Criterion benchmarks for the result-set parser/decoder pipeline.
//! Build with `--features bench-internals`.

use std::hint::black_box;
use std::io::Write;
use std::sync::Arc;

use bytes::Bytes;
use chrono::NaiveDateTime;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use flate2::{Compression, write::GzEncoder};

use snowflake_connector_rs::bench_support::{
    decode_gzip_chunk, make_result_table_from_rows, make_schema, parse_remote_chunk_result_table,
    parse_remote_chunk_result_table_async_with_workload, parse_statement_envelope,
};
use snowflake_connector_rs::{ColumnType, DynamicRow, Schema};

#[derive(snowflake_connector_rs::FromRow)]
struct BenchRow {
    id: i64,
    name: String,
    ts: NaiveDateTime,
}

fn synthetic_chunk_bytes(row_count: usize, null_pct: u8, escaped_pct: u8) -> Bytes {
    // remote chunk fragment shape: `[..],[..],[..]`. Each row is
    // [int_text, string_text, timestamp_text].
    let mut out = Vec::with_capacity(row_count * 64);
    for i in 0..row_count {
        if i > 0 {
            out.push(b',');
        }
        out.push(b'[');

        // integer
        out.push(b'"');
        out.extend_from_slice(format!("{i}").as_bytes());
        out.push(b'"');
        out.push(b',');

        // string: "name_<i>" with optional escape sequence
        if (i as u8 % 100) < null_pct {
            out.extend_from_slice(b"null");
        } else if (i as u8 % 100) < null_pct + escaped_pct {
            out.extend_from_slice(b"\"esc\\n_");
            out.extend_from_slice(format!("{i}").as_bytes());
            out.push(b'"');
        } else {
            out.push(b'"');
            out.extend_from_slice(format!("name_{i}").as_bytes());
            out.push(b'"');
        }
        out.push(b',');

        // timestamp
        out.push(b'"');
        out.extend_from_slice(format!("{}.123456789", 1_700_000_000 + i as i64).as_bytes());
        out.push(b'"');
        out.push(b']');
    }
    Bytes::from(out)
}

fn synthetic_chunk_bytes_at_least(
    target_bytes: usize,
    null_pct: u8,
    escaped_pct: u8,
) -> (Bytes, usize) {
    let mut rows = 1_000usize;
    loop {
        let bytes = synthetic_chunk_bytes(rows, null_pct, escaped_pct);
        if bytes.len() >= target_bytes {
            return (bytes, rows);
        }
        rows *= 2;
    }
}

fn gzip_bytes(bytes: &Bytes) -> Bytes {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).unwrap();
    Bytes::from(encoder.finish().unwrap())
}

fn synthetic_inline_rowset_bytes(row_count: usize, null_pct: u8, escaped_pct: u8) -> Bytes {
    let rows = synthetic_chunk_bytes(row_count, null_pct, escaped_pct);
    let mut out = Vec::with_capacity(rows.len() + 2);
    out.push(b'[');
    out.extend_from_slice(&rows);
    out.push(b']');
    Bytes::from(out)
}

fn synthetic_statement_response_bytes(row_count: usize, null_pct: u8, escaped_pct: u8) -> Bytes {
    let rowset = synthetic_inline_rowset_bytes(row_count, null_pct, escaped_pct);
    let mut out = Vec::with_capacity(rowset.len() + 512);
    out.extend_from_slice(br#"{"data":{"queryId":"bench","returned":"#);
    out.extend_from_slice(row_count.to_string().as_bytes());
    out.extend_from_slice(br#","total":"#);
    out.extend_from_slice(row_count.to_string().as_bytes());
    out.extend_from_slice(br#","rowset":"#);
    out.extend_from_slice(&rowset);
    out.extend_from_slice(br#","rowtype":[{"database":"","name":"ID","nullable":false,"scale":0,"byteLength":0,"length":0,"schema":"","table":"","precision":38,"type":"FIXED"},{"database":"","name":"NAME","nullable":true,"scale":0,"byteLength":0,"length":16777216,"schema":"","table":"","precision":0,"type":"TEXT"},{"database":"","name":"TS","nullable":false,"scale":9,"byteLength":0,"length":0,"schema":"","table":"","precision":0,"type":"TIMESTAMP_NTZ"}],"queryResultFormat":"json"},"success":true}"#);
    Bytes::from(out)
}

fn build_schema() -> Arc<Schema> {
    make_schema(vec![
        (
            "ID".to_string(),
            ColumnType::Fixed {
                precision: None,
                scale: Some(0),
            },
            false,
        ),
        ("NAME".to_string(), ColumnType::Text { length: None }, true),
        (
            "TS".to_string(),
            ColumnType::TimestampNtz { scale: Some(9) },
            false,
        ),
    ])
}

fn bench_chunk_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("remote_chunk_to_table");
    let schema = build_schema();
    for &rows in &[1_000usize, 10_000, 100_000] {
        let bytes = synthetic_chunk_bytes(rows, 5, 1);
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                let s = Arc::clone(&schema);
                let body = bytes.clone();
                let t = parse_remote_chunk_result_table(s, Arc::from("bench"), body).unwrap();
                black_box(t);
            })
        });
    }
    group.finish();
}

fn bench_async_chunk_parse_boundary(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("remote_chunk_to_table_async_wrapper");
    let schema = build_schema();
    for &(label, target_bytes) in &[
        ("1MiB", 1024 * 1024usize),
        ("8MiB", 8 * 1024 * 1024usize),
        ("32MiB", 32 * 1024 * 1024usize),
    ] {
        let (bytes, rows) = synthetic_chunk_bytes_at_least(target_bytes, 5, 1);
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(label),
            &(bytes, rows),
            |b, (bytes, rows)| {
                b.iter(|| {
                    let table = rt
                        .block_on(parse_remote_chunk_result_table_async_with_workload(
                            Arc::clone(&schema),
                            Arc::from("bench"),
                            bytes.clone(),
                            Some(*rows as u64),
                            Some(bytes.len()),
                        ))
                        .unwrap();
                    black_box(table);
                })
            },
        );
    }
    group.finish();
}

fn bench_statement_envelope_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("statement_envelope_parse");
    for &rows in &[1_000usize, 10_000, 100_000] {
        let body = synthetic_statement_response_bytes(rows, 5, 1);
        group.throughput(Throughput::Bytes(body.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(rows), &body, |b, body| {
            b.iter(|| {
                let parts = parse_statement_envelope(body.clone()).unwrap();
                black_box(parts);
            })
        });
    }
    group.finish();
}

fn bench_gzip_plus_chunk_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("gzip_plus_chunk_parse");
    let schema = build_schema();
    for &rows in &[1_000usize, 10_000, 100_000] {
        let chunk = synthetic_chunk_bytes(rows, 5, 1);
        let compressed = gzip_bytes(&chunk);
        group.throughput(Throughput::Bytes(compressed.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(rows),
            &compressed,
            |b, compressed| {
                b.iter(|| {
                    let decoded = decode_gzip_chunk(compressed.clone()).unwrap();
                    let table = parse_remote_chunk_result_table(
                        Arc::clone(&schema),
                        Arc::from("bench"),
                        decoded,
                    )
                    .unwrap();
                    black_box(table);
                })
            },
        );
    }
    group.finish();
}

fn bench_decode_tuple(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_rows_tuple_decode");
    let schema = build_schema();
    for &rows in &[1_000usize, 10_000] {
        let bytes = synthetic_chunk_bytes(rows, 0, 0);
        let table = parse_remote_chunk_result_table(Arc::clone(&schema), Arc::from("bench"), bytes)
            .unwrap();
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                let mut sum: i64 = 0;
                for row in table.rows::<(i64, String, NaiveDateTime)>().unwrap() {
                    let (id, _, _) = row.unwrap();
                    sum = sum.wrapping_add(id);
                }
                black_box(sum);
            })
        });
    }
    group.finish();
}

fn bench_decode_derive(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_rows_derive_decode");
    let schema = build_schema();
    for &rows in &[1_000usize, 10_000] {
        let bytes = synthetic_chunk_bytes(rows, 0, 0);
        let table = parse_remote_chunk_result_table(Arc::clone(&schema), Arc::from("bench"), bytes)
            .unwrap();
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                let mut sum: i64 = 0;
                for row in table.rows::<BenchRow>().unwrap() {
                    let row = row.unwrap();
                    sum = sum
                        .wrapping_add(row.id)
                        .wrapping_add(row.name.len() as i64)
                        .wrapping_add(row.ts.and_utc().timestamp());
                }
                black_box(sum);
            })
        });
    }
    group.finish();
}

fn bench_decode_dynamic(c: &mut Criterion) {
    let mut group = c.benchmark_group("dynamic_rows_decode");
    let schema = build_schema();
    for &rows in &[1_000usize, 10_000] {
        let bytes = synthetic_chunk_bytes(rows, 0, 0);
        let table = parse_remote_chunk_result_table(Arc::clone(&schema), Arc::from("bench"), bytes)
            .unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                let n: usize = table
                    .dynamic_rows()
                    .unwrap()
                    .filter_map(Result::ok)
                    .map(|r: DynamicRow| r.values().len())
                    .sum();
                black_box(n);
            })
        });
    }
    group.finish();
}

fn bench_make_result_table_from_rows(c: &mut Criterion) {
    let schema = build_schema();
    let rows = (0..10_000)
        .map(|i| {
            vec![
                Some(format!("{i}")),
                Some(format!("name_{i}")),
                Some(format!("{}.000000000", 1_700_000_000 + i as i64)),
            ]
        })
        .collect::<Vec<_>>();
    c.bench_function("inline_rows_to_result_table_10k", |b| {
        b.iter(|| {
            let t = make_result_table_from_rows(Arc::clone(&schema), rows.clone()).unwrap();
            black_box(t);
        })
    });
}

criterion_group!(
    benches,
    bench_chunk_parse,
    bench_async_chunk_parse_boundary,
    bench_statement_envelope_parse,
    bench_gzip_plus_chunk_parse,
    bench_decode_tuple,
    bench_decode_derive,
    bench_decode_dynamic,
    bench_make_result_table_from_rows,
);
criterion_main!(benches);
