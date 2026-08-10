//! Integration and unit tests for Goy Node Onboarding & Offboarding workflows.

use goy_node::goy_api::validate_auth_key;
use goy_node::onboard::{check_onboard_status, run_offboard, run_onboard};
use tempfile::tempdir;

#[test]
fn test_auth_key_validation_rules() {
    assert!(validate_auth_key("gc_9999999999"));
    assert!(validate_auth_key("gc_company_secret_auth_key_2026"));

    assert!(!validate_auth_key("invalid_prefix_key"));
    assert!(!validate_auth_key("gc_123")); // too short
    assert!(!validate_auth_key(""));
}

#[tokio::test]
async fn test_onboard_non_interactive_vpn_only_flow() -> anyhow::Result<()> {
    if std::process::Command::new("tailscale")
        .arg("version")
        .output()
        .is_err()
    {
        eprintln!("⏭️  Skipping test_onboard_non_interactive_vpn_only_flow: tailscale CLI not available");
        return Ok(());
    }

    unsafe {
        std::env::set_var("GOY_API_MOCK", "1");
    }

    let dir = tempdir()?;
    let data_dir = dir.path().join("data");
    let config_path = dir.path().join("config.toml");

    let auth_key = "gc_test_automation_key_12345".to_string();

    // Executar onboarding não-interativo em modo --vpn-only
    let code = run_onboard(
        Some(auth_key.clone()),
        true, // non_interactive
        true, // vpn_only
        Some(&config_path),
        Some(&data_dir),
    )
    .await?;

    assert_eq!(code, 0, "Onboarding should complete with exit code 0");

    // Verificar que os ficheiros foram criados
    assert!(config_path.exists(), "config.toml must be generated");
    assert!(
        data_dir.join("onboard_state.json").exists(),
        "onboard_state.json must exist"
    );
    assert!(
        data_dir.join("vpn_state.json").exists(),
        "vpn_state.json must exist"
    );
    assert!(
        data_dir.join("node_id.txt").exists(),
        "node_id.txt must exist"
    );

    // Verificar leitura do estado
    let status = check_onboard_status(Some(&data_dir));
    assert!(status.is_some(), "check_onboard_status must return state");

    unsafe {
        std::env::remove_var("GOY_API_MOCK");
    }

    Ok(())
}

#[tokio::test]
async fn test_onboard_invalid_auth_key_returns_code_2() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let data_dir = dir.path().join("data");
    let config_path = dir.path().join("config.toml");

    let code = run_onboard(
        Some("invalid_key".to_string()),
        true,
        false,
        Some(&config_path),
        Some(&data_dir),
    )
    .await?;

    assert_eq!(code, 2, "Invalid auth key must return exit code 2");
    assert!(!data_dir.join("onboard_state.json").exists());

    Ok(())
}

#[tokio::test]
async fn test_offboard_cleans_state_files() -> anyhow::Result<()> {
    if std::process::Command::new("tailscale")
        .arg("version")
        .output()
        .is_err()
    {
        eprintln!("⏭️  Skipping test_offboard_cleans_state_files: tailscale CLI not available");
        return Ok(());
    }

    unsafe {
        std::env::set_var("GOY_API_MOCK", "1");
    }

    let dir = tempdir()?;
    let data_dir = dir.path().join("data");
    let config_path = dir.path().join("config.toml");

    // 1. Onboard primeiro
    let _ = run_onboard(
        Some("gc_test_key_offboard_12345".to_string()),
        true,
        true,
        Some(&config_path),
        Some(&data_dir),
    )
    .await?;

    assert!(data_dir.join("onboard_state.json").exists());

    // 2. Offboard com force=true
    let code = run_offboard(true, Some(&config_path), Some(&data_dir)).await?;
    assert_eq!(code, 0);

    // Ficheiros de estado devem ter sido removidos
    assert!(!data_dir.join("onboard_state.json").exists());
    assert!(!data_dir.join("vpn_state.json").exists());
    assert!(check_onboard_status(Some(&data_dir)).is_none());

    unsafe {
        std::env::remove_var("GOY_API_MOCK");
    }

    Ok(())
}
