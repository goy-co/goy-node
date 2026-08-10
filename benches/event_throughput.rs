//! Criterion benchmark suite for Goy Node event processing & replication throughput.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use dashmap::DashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use goy_node::consistent_hash::ConsistentHashRing;
use goy_node::metrics::Metrics;

fn bench_event_dedup(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_dedup");
    let seen_ids: DashSet<String> = DashSet::new();

    for i in 0..10_000 {
        seen_ids.insert(format!("prefilled_evt_{i}"));
    }

    group.throughput(Throughput::Elements(1));
    let mut counter = 10_000u64;

    group.bench_function("dashset_insert_dedup", |b| {
        b.iter(|| {
            counter += 1;
            let evt_id = format!("evt_{counter}");
            seen_ids.insert(evt_id)
        })
    });

    group.bench_function("dashset_contains_lookup", |b| {
        b.iter(|| seen_ids.contains("prefilled_evt_5000"))
    });

    group.finish();
}

fn bench_hash_ring_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("consistent_hash_ring");
    let mut ring = ConsistentHashRing::new(150);

    for i in 0..10 {
        ring.add_peer(&format!("ws://node-{i}:8443"));
    }

    group.throughput(Throughput::Elements(1));

    group.bench_function("get_responsible_peers_rf3", |b| {
        b.iter(|| ring.get_responsible_peers("evt_benchmark_key_12345", 3))
    });

    group.bench_function("get_primary_peer", |b| {
        b.iter(|| ring.get_primary_peer("evt_benchmark_key_12345"))
    });

    group.finish();
}

fn bench_metrics_counters(c: &mut Criterion) {
    let mut group = c.benchmark_group("metrics_atomic_ops");
    let metrics = Arc::new(Metrics::new());

    group.throughput(Throughput::Elements(1));

    group.bench_function("inc_events_received", |b| {
        b.iter(|| {
            metrics.events_received_peer.fetch_add(1, Ordering::Relaxed);
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_event_dedup,
    bench_hash_ring_lookup,
    bench_metrics_counters
);
criterion_main!(benches);
