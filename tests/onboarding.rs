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
        eprintln!(
            "⏭️  Skipping test_onboard_non_interactive_vpn_only_flow: tailscale CLI not available"
        );
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

#[tokio::test]
async fn test_onboard_storage_verification_failure_returns_code_5() -> anyhow::Result<()> {
    let invalid_dir = std::path::PathBuf::from("/proc/impossible_dir_goy_test");
    let config_path = std::path::PathBuf::from("/tmp/config_test.toml");

    let code = run_onboard(
        Some("gc_valid_test_auth_key_12345".to_string()),
        true,  // non-interactive
        false, // vpn_only
        Some(&config_path),
        Some(&invalid_dir),
    )
    .await?;

    assert_eq!(
        code,
        goy_node::onboard::EXIT_ONBOARD_STORAGE_ERROR,
        "Storage failure must return exit code 5"
    );
    assert!(!invalid_dir.join("onboard_state.json").exists());

    Ok(())
}

#[tokio::test]
async fn test_onboard_storage_failure_preserves_data_dir_structure() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let data_dir = dir.path().join("data_onboard_storage_fail");

    // Forçar falha de escrita tornando o diretório read-only após a sua criação
    std::fs::create_dir_all(&data_dir)?;
    let mut perms = std::fs::metadata(&data_dir)?.permissions();
    perms.set_readonly(true);
    let _ = std::fs::set_permissions(&data_dir, perms.clone());

    let code = run_onboard(
        Some("gc_valid_test_auth_key_12345".to_string()),
        true,
        false,
        None,
        Some(&data_dir),
    )
    .await?;

    // Restaurar permissões para permitir limpeza do tempdir
    perms.set_readonly(false);
    let _ = std::fs::set_permissions(&data_dir, perms);

    assert_eq!(
        code,
        goy_node::onboard::EXIT_ONBOARD_STORAGE_ERROR,
        "Read-only storage failure must return exit code 5"
    );
    assert!(data_dir.exists(), "Created directory must be preserved");
    assert!(
        !data_dir.join("onboard_state.json").exists(),
        "State file must NOT be written when storage fails"
    );

    Ok(())
}
