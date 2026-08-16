//! Integration tests for binary CLI flags and configuration commands.

use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_help_output() {
    Command::cargo_bin("goy-node")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--coord-url"))
        .stdout(predicate::str::contains("--admin-api-key"))
        .stdout(predicate::str::contains("--no-interactive"))
        .stdout(predicate::str::contains("--log-level"))
        .stdout(predicate::str::contains("--log-format"));
}

#[test]
fn test_version_output() {
    Command::cargo_bin("goy-node")
        .unwrap()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("goy-node"));
}

#[test]
fn test_no_interactive_without_config_fails() {
    let dir = tempfile::TempDir::new().unwrap();
    let nonexistent_cfg = dir.path().join("nonexistent.toml");

    Command::cargo_bin("goy-node")
        .unwrap()
        .args([
            "--config",
            nonexistent_cfg.to_str().unwrap(),
            "--no-interactive",
            "run",
        ])
        .env_remove("GOY_API_URL")
        .env_remove("GOY_ADMIN_API_KEY")
        .assert()
        .failure()
        .stderr(predicate::str::contains("coord.admin_api_key"));
}

#[test]
fn test_config_validate_with_valid_file() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        include_str!("../src/config/default_config.toml")
            .replace("admin_api_key = \"\"", "admin_api_key = \"test_key\""),
    )
    .unwrap();

    Command::cargo_bin("goy-node")
        .unwrap()
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "config",
            "validate",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("valid"))
        .stdout(predicate::str::contains("Sources:"));
}

#[test]
fn test_config_show_with_masked_secrets() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        include_str!("../src/config/default_config.toml").replace(
            "admin_api_key = \"\"",
            "admin_api_key = \"supersecret12345\"",
        ),
    )
    .unwrap();

    Command::cargo_bin("goy-node")
        .unwrap()
        .args(["--config", config_path.to_str().unwrap(), "config", "show"])
        .assert()
        .success()
        .stdout(predicate::str::contains("supe****"))
        .stdout(predicate::str::contains("supersecret12345").not());
}

#[test]
fn test_config_init_non_interactive() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("config.toml");

    Command::cargo_bin("goy-node")
        .unwrap()
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "config",
            "init",
            "--coord-url",
            "http://test:8080",
            "--admin-api-key",
            "test_key_12345",
            "--non-interactive",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("saved"));

    assert!(config_path.exists());
    let content = std::fs::read_to_string(&config_path).unwrap();
    assert!(content.contains("http://test:8080"));
    assert!(content.contains("test_key_12345"));
}

#[test]
fn test_config_set_and_get() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("config.toml");

    // 1. Init
    Command::cargo_bin("goy-node")
        .unwrap()
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "config",
            "init",
            "--coord-url",
            "http://test:8080",
            "--admin-api-key",
            "test_key_12345",
            "--non-interactive",
        ])
        .assert()
        .success();

    // 2. Set field
    Command::cargo_bin("goy-node")
        .unwrap()
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "config",
            "set",
            "coord.url",
            "http://new:9090",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Updated"));

    // 3. Get field
    Command::cargo_bin("goy-node")
        .unwrap()
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "config",
            "get",
            "coord.url",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("http://new:9090"));

    // 4. Get masked secret
    Command::cargo_bin("goy-node")
        .unwrap()
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "config",
            "get",
            "coord.admin_api_key",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("test****"))
        .stdout(predicate::str::contains("test_key_12345").not());
}

#[test]
fn test_config_init_refuses_overwrite_without_force() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(&config_path, "existing content").unwrap();

    Command::cargo_bin("goy-node")
        .unwrap()
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "config",
            "init",
            "--coord-url",
            "http://test:8080",
            "--admin-api-key",
            "test_key_12345",
            "--non-interactive",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("already exists"));

    // Com --force deve funcionar
    Command::cargo_bin("goy-node")
        .unwrap()
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "config",
            "init",
            "--coord-url",
            "http://test:8080",
            "--admin-api-key",
            "test_key_12345",
            "--non-interactive",
            "--force",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("saved"));
}

#[test]
fn test_config_set_invalid_field_fails() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("config.toml");

    Command::cargo_bin("goy-node")
        .unwrap()
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "config",
            "init",
            "--coord-url",
            "http://test:8080",
            "--admin-api-key",
            "test_key_12345",
            "--non-interactive",
        ])
        .assert()
        .success();

    Command::cargo_bin("goy-node")
        .unwrap()
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "config",
            "set",
            "invalid.field",
            "some_val",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Unknown config field"));
}

#[test]
fn test_first_run_auto_generate_cli() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("config.toml");

    Command::cargo_bin("goy-node")
        .unwrap()
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "--coord-url",
            "http://10.0.0.5:8080",
            "--admin-api-key",
            "auto_secret_12345",
            "--no-interactive",
            "config",
            "show",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("http://10.0.0.5:8080"))
        .stdout(predicate::str::contains("auto****"));

    assert!(config_path.exists());
    let content = std::fs::read_to_string(&config_path).unwrap();
    assert!(content.contains("Auto-generated on first run"));
    assert!(content.contains("http://10.0.0.5:8080"));
    assert!(content.contains("auto_secret_12345"));
}

#[test]
fn test_non_interactive_without_config_fails_cleanly() {
    Command::cargo_bin("goy-node")
        .unwrap()
        .args([
            "--no-interactive",
            "onboard",
            "--auth-key",
            "gc_test_1234567890",
        ])
        .env_remove("GOY_API_URL")
        .env_remove("GOY_ADMIN_API_KEY")
        .assert()
        .failure()
        .stderr(predicate::str::contains("non-interactive"))
        .stderr(predicate::str::contains("--coord-url"))
        .stderr(predicate::str::contains("--admin-api-key"));
}

#[test]
fn test_non_interactive_with_all_flags_succeeds() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("config.toml");

    Command::cargo_bin("goy-node")
        .unwrap()
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "--coord-url",
            "http://test:8080",
            "--admin-api-key",
            "test_key_12345",
            "--no-interactive",
            "config",
            "validate",
        ])
        .assert()
        .success();
}

#[test]
fn test_piped_input_fails_gracefully() {
    Command::cargo_bin("goy-node")
        .unwrap()
        .args(["onboard", "--auth-key", "gc_test_1234567890"])
        .env_remove("GOY_API_URL")
        .env_remove("GOY_ADMIN_API_KEY")
        .write_stdin("")
        .assert()
        .failure()
        .stderr(predicate::str::contains("not a terminal"));
}

#[test]
fn test_config_init_force_overwrites() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(&config_path, "old content").unwrap();

    Command::cargo_bin("goy-node")
        .unwrap()
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "config",
            "init",
            "--coord-url",
            "http://new:8080",
            "--admin-api-key",
            "new_key_12345",
            "--non-interactive",
            "--force",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("saved"));

    let content = std::fs::read_to_string(&config_path).unwrap();
    assert!(content.contains("http://new:8080"));
    assert!(!content.contains("old content"));
}
