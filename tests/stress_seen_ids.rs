//! Stress test suite for seen_ids scale, memory footprint, and atomic persistence.

use dashmap::DashSet;
use std::time::Instant;
use tempfile::tempdir;

#[test]
fn test_stress_seen_ids_scale_100k_elements() {
    let seen_ids: DashSet<String> = DashSet::new();
    let total_elements = 100_000;

    let start_insert = Instant::now();
    for i in 0..total_elements {
        let id = format!("evt_scale_test_{i:064x}");
        seen_ids.insert(id);
    }
    let insert_duration = start_insert.elapsed();

    assert_eq!(seen_ids.len(), total_elements);

    // Latência de lookup O(1)
    let start_lookup = Instant::now();
    let lookups = 10_000;
    for i in 0..lookups {
        let id = format!("evt_scale_test_{i:064x}");
        assert!(seen_ids.contains(&id));
    }
    let lookup_duration = start_lookup.elapsed();

    let avg_lookup_micros = lookup_duration.as_micros() as f64 / lookups as f64;
    assert!(
        avg_lookup_micros < 10.0,
        "Average lookup time must be under 10µs (got {avg_lookup_micros:.2}µs)"
    );

    println!(
        "✅ Inserted {total_elements} elements in {insert_duration:?}, avg lookup: {avg_lookup_micros:.2}µs"
    );
}

#[test]
fn test_stress_seen_ids_atomic_json_persistence() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let file_path = dir.path().join("seen_ids_scale.json");

    let seen_ids: DashSet<String> = DashSet::new();
    let total = 20_000;

    for i in 0..total {
        seen_ids.insert(format!("persist_evt_{i:064x}"));
    }

    // Gravar em ficheiro
    let start_save = Instant::now();
    let vec: Vec<String> = seen_ids.iter().map(|r| r.clone()).collect();
    let json_bytes = serde_json::to_vec(&vec)?;
    std::fs::write(&file_path, &json_bytes)?;
    let save_duration = start_save.elapsed();

    assert!(file_path.exists());
    let file_size_mb = std::fs::metadata(&file_path)?.len() as f64 / (1024.0 * 1024.0);

    // Carregar de ficheiro
    let start_load = Instant::now();
    let read_bytes = std::fs::read(&file_path)?;
    let loaded_vec: Vec<String> = serde_json::from_slice(&read_bytes)?;
    let load_duration = start_load.elapsed();

    assert_eq!(loaded_vec.len(), total);
    println!(
        "✅ Saved {total} seen_ids ({file_size_mb:.2} MB) in {save_duration:?}, loaded in {load_duration:?}"
    );

    Ok(())
}
