#[cfg(test)]
mod tests {
    use crate::config::commands::*;
    use crate::config::schema::*;
    use tempfile::TempDir;

    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn valid_test_config() -> GoyNodeConfig {
        GoyNodeConfig {
            coord: crate::config::schema::CoordConfig {
                url: "http://localhost:8080".to_string(),
                admin_api_key: "valid_admin_key_12345".to_string(),
                heartbeat_interval_secs: 60,
                request_timeout_secs: 10,
            },
            relay: crate::config::schema::RelayConfig {
                url: "ws://127.0.0.1:7777".to_string(),
                import_cmd: None,
            },
            mesh: crate::config::schema::MeshConfig {
                listen: "0.0.0.0:8443".to_string(),
                seeds: vec![],
                registry_url: None,
                heartbeat_secs: 30,
                tls_enabled: true,
                trusted_fingerprints: Default::default(),
            },
            storage: crate::config::schema::StorageConfig {
                data_dir: std::path::PathBuf::from("/var/lib/goy-node"),
                extra_contribution_gb: 0,
            },
            metrics: crate::config::schema::MetricsConfig {
                listen: "127.0.0.1:9090".to_string(),
            },
            log: crate::config::schema::LogConfig {
                level: "info".to_string(),
                format: "pretty".to_string(),
            },
        }
    }

    #[test]
    fn test_init_non_interactive_creates_config() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("goy-node").join("config.toml");

        let args = InitArgs {
            coord_url: Some("http://test:8080".to_string()),
            admin_api_key: Some("test_key_12345".to_string()),
            data_dir: Some(dir.path().join("data")),
            non_interactive: true,
            ..Default::default()
        };

        let config = build_config_non_interactive(&args).unwrap();
        assert_eq!(config.coord.url, "http://test:8080");
        assert_eq!(config.coord.admin_api_key, "test_key_12345");
        config.validate().unwrap();

        write_config(&config_path, &config).unwrap();
        assert!(config_path.exists());
    }

    #[test]
    fn test_init_non_interactive_fails_without_admin_key() {
        let args = InitArgs {
            coord_url: Some("http://test:8080".to_string()),
            non_interactive: true,
            ..Default::default()
        };
        let result = build_config_non_interactive(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("admin-api-key"));
    }

    #[test]
    fn test_set_valid_field() {
        let mut config = valid_test_config();
        apply_set_field(&mut config, "coord.url", "http://new:9090").unwrap();
        assert_eq!(config.coord.url, "http://new:9090");
    }

    #[test]
    fn test_set_invalid_field() {
        let mut config = valid_test_config();
        let result = apply_set_field(&mut config, "invalid.field", "value");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown config field"));
    }

    #[test]
    fn test_set_invalid_type() {
        let mut config = valid_test_config();
        let result = apply_set_field(&mut config, "coord.heartbeat_interval_secs", "not_a_number");
        assert!(result.is_err());
    }

    #[test]
    fn test_set_seeds_array() {
        let mut config = valid_test_config();
        apply_set_field(&mut config, "mesh.seeds", r#"["ws://a:8443","ws://b:8443"]"#).unwrap();
        assert_eq!(config.mesh.seeds.len(), 2);
    }

    #[test]
    fn test_get_valid_field() {
        let config = valid_test_config();
        let value = get_field(&config, "coord.url").unwrap();
        assert_eq!(value, "http://localhost:8080");
    }

    #[test]
    fn test_get_unknown_field() {
        let config = valid_test_config();
        let result = get_field(&config, "unknown.field");
        assert!(result.is_err());
    }

    #[test]
    fn test_write_config_permissions() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        let config = valid_test_config();
        write_config(&path, &config).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::metadata(&path).unwrap().permissions();
            assert_eq!(perms.mode() & 0o777, 0o600);
        }
    }

    #[test]
    fn test_set_invalid_value_does_not_modify_file() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("config.toml");
        let config = valid_test_config();
        write_config(&config_path, &config).unwrap();

        let original_content = std::fs::read_to_string(&config_path).unwrap();

        let args = SetArgs {
            key: "coord.heartbeat_interval_secs".to_string(),
            value: "not_a_number".to_string(),
        };

        let result = cmd_set(&args, Some(&config_path));
        assert!(result.is_err());

        // Verificar que ficheiro em disco não foi modificado
        let disk_content = std::fs::read_to_string(&config_path).unwrap();
        assert_eq!(disk_content, original_content);
    }

    #[test]
    fn test_get_seeds_formats_as_array() {
        let mut config = valid_test_config();
        config.mesh.seeds = vec!["ws://seed1:8443".to_string(), "ws://seed2:8443".to_string()];
        let value = get_field(&config, "mesh.seeds").unwrap();
        assert!(value.contains("ws://seed1:8443"));
        assert!(value.contains("ws://seed2:8443"));
    }

    #[test]
    fn test_concurrent_config_set() {
        use std::sync::Arc;
        use std::thread;

        let dir = TempDir::new().unwrap();
        let config_path = Arc::new(dir.path().join("config.toml"));
        let config = valid_test_config();
        write_config(&config_path, &config).unwrap();

        let mut handles = vec![];
        for i in 0..5 {
            let path = Arc::clone(&config_path);
            handles.push(thread::spawn(move || {
                let args = SetArgs {
                    key: "coord.heartbeat_interval_secs".to_string(),
                    value: (30 + i).to_string(),
                };
                let _ = cmd_set(&args, Some(&path));
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Ficheiro resultante deve continuar TOML válido e com permissões corretas
        let content = std::fs::read_to_string(&*config_path).unwrap();
        let parsed: Result<GoyNodeConfig, _> = toml::from_str(&content);
        assert!(parsed.is_ok());
    }

    #[test]
    fn test_migrate_finds_and_converts_env_vars() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("config.toml");

        unsafe {
            std::env::set_var("GOY_API_URL", "http://migrate-test:8080");
            std::env::set_var("GOY_ADMIN_API_KEY", "migrate_key_12345");
        }

        let found = crate::config::resolver::scan_deprecated_env_vars();
        assert!(found.len() >= 2);
        assert!(found.iter().any(|(ev, _, _)| *ev == "GOY_API_URL"));
        assert!(found.iter().any(|(ev, _, _)| *ev == "GOY_ADMIN_API_KEY"));

        // Executar cmd_migrate não-interativo
        let res = cmd_migrate(true, Some(&config_path));
        assert!(res.is_ok());

        let disk_content = std::fs::read_to_string(&config_path).unwrap();
        assert!(disk_content.contains("http://migrate-test:8080"));
        assert!(disk_content.contains("migrate_key_12345"));

        unsafe {
            std::env::remove_var("GOY_API_URL");
            std::env::remove_var("GOY_ADMIN_API_KEY");
        }
    }

    #[test]
    fn test_migrate_no_env_vars_found() {
        let _lock = ENV_MUTEX.lock().unwrap();
        for (ev, _, _) in crate::config::resolver::ENV_VAR_DEPRECATIONS {
            unsafe {
                std::env::remove_var(ev);
            }
        }

        let found = crate::config::resolver::scan_deprecated_env_vars();
        assert!(found.is_empty());
    }
}
