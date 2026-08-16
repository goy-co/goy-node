#[cfg(test)]
mod tests {
    use crate::config::resolver::*;
    use crate::config::schema::GoyNodeConfig;
    use tempfile::TempDir;

    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_cli_flag_overrides_config_file() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(
            &config_path,
            r#"
            [coord]
            url = "http://from-file:8080"
            admin_api_key = "file_key"
            [relay]
            url = "ws://127.0.0.1:7777"
            [mesh]
            listen = "0.0.0.0:8443"
            [storage]
            data_dir = "/var/lib/goy-node"
            [metrics]
            listen = "127.0.0.1:9090"
        "#,
        )
        .unwrap();

        let opts = ResolveOptions {
            config_path: Some(config_path),
            coord_url: Some("http://from-cli:9090".to_string()),
            ..Default::default()
        };

        let resolved = resolve(&opts).unwrap();
        assert_eq!(resolved.config.coord.url, "http://from-cli:9090");
        assert_eq!(
            resolved.sources.get("coord.url"),
            Some(&ConfigSource::CliFlag("coord-url".to_string()))
        );
    }

    #[test]
    fn test_config_file_overrides_env_var() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(
            &config_path,
            r#"
            [coord]
            url = "http://from-file:8080"
            admin_api_key = "file_key"
            [relay]
            url = "ws://127.0.0.1:7777"
            [mesh]
            listen = "0.0.0.0:8443"
            [storage]
            data_dir = "/var/lib/goy-node"
            [metrics]
            listen = "127.0.0.1:9090"
        "#,
        )
        .unwrap();

        unsafe {
            std::env::set_var("GOY_API_URL", "http://from-env:8080");
        }
        let opts = ResolveOptions {
            config_path: Some(config_path),
            no_interactive: true,
            ..Default::default()
        };

        let resolved = resolve(&opts).unwrap();
        // Config file ganha sobre env var
        assert_eq!(resolved.config.coord.url, "http://from-file:8080");
        assert!(resolved.warnings.iter().any(|w| w.contains("deprecated")));

        unsafe {
            std::env::remove_var("GOY_API_URL");
        }
    }

    #[test]
    fn test_no_interactive_fails_on_missing_required() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let nonexistent_path = dir.path().join("nonexistent.toml");

        let opts = ResolveOptions {
            config_path: Some(nonexistent_path),
            no_interactive: true,
            ..Default::default()
        };

        let result = resolve(&opts);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("coord.url") || err_msg.contains("coord.admin_api_key"));
    }

    #[test]
    fn test_mask_secret() {
        assert_eq!(mask_secret("abcdef1234"), "abcd****");
        assert_eq!(mask_secret("ab"), "****");
        assert_eq!(mask_secret(""), "****");
    }

    #[test]
    fn test_sources_populated_for_all_fields() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(
            &config_path,
            include_str!("default_config.toml")
                .replace("admin_api_key = \"\"", "admin_api_key = \"test_key\""),
        )
        .unwrap();

        let opts = ResolveOptions {
            config_path: Some(config_path),
            no_interactive: true,
            ..Default::default()
        };

        let resolved = resolve(&opts).unwrap();
        assert!(resolved.sources.contains_key("coord.url"));
        assert!(resolved.sources.contains_key("mesh.listen"));
        assert!(resolved.sources.contains_key("log.level"));
    }

    #[test]
    fn test_auto_generate_creates_config_on_first_run() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("config.toml");

        let opts = ResolveOptions {
            config_path: Some(config_path.clone()),
            coord_url: Some("http://auto:8080".to_string()),
            admin_api_key: Some("auto_key_12345".to_string()),
            no_interactive: true,
            ..Default::default()
        };

        let resolved = resolve(&opts).unwrap();
        assert!(config_path.exists());
        assert_eq!(resolved.config.coord.url, "http://auto:8080");
        assert_eq!(resolved.config.coord.admin_api_key, "auto_key_12345");
        assert!(resolved
            .warnings
            .iter()
            .any(|w| w.contains("Configuration saved") || w.contains("Auto-generated")));
    }

    #[test]
    fn test_auto_generate_does_not_overwrite_existing() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(
            &config_path,
            r#"
            [coord]
            url = "http://existing:8080"
            admin_api_key = "existing_key"
            [relay]
            url = "ws://127.0.0.1:7777"
            [mesh]
            listen = "0.0.0.0:8443"
            [storage]
            data_dir = "/var/lib/goy-node"
            [metrics]
            listen = "127.0.0.1:9090"
        "#,
        )
        .unwrap();

        let opts = ResolveOptions {
            config_path: Some(config_path.clone()),
            coord_url: Some("http://should-not-override:8080".to_string()),
            no_interactive: true,
            ..Default::default()
        };

        let resolved = resolve(&opts).unwrap();
        // CLI flag sobrescreve em memória
        assert_eq!(resolved.config.coord.url, "http://should-not-override:8080");
        // O ficheiro em disco mantém o valor original
        let disk_content = std::fs::read_to_string(&config_path).unwrap();
        assert!(disk_content.contains("http://existing:8080"));
    }

    #[test]
    fn test_auto_generate_non_interactive_fails_without_required() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("config.toml");

        let opts = ResolveOptions {
            config_path: Some(config_path),
            no_interactive: true,
            ..Default::default()
        };

        let result = resolve(&opts);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("coord.admin_api_key"));
    }

    #[test]
    fn test_auto_generated_config_has_correct_permissions() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("config.toml");

        let opts = ResolveOptions {
            config_path: Some(config_path.clone()),
            coord_url: Some("http://test:8080".to_string()),
            admin_api_key: Some("test_key".to_string()),
            no_interactive: true,
            ..Default::default()
        };

        resolve(&opts).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::metadata(&config_path).unwrap().permissions();
            assert_eq!(perms.mode() & 0o777, 0o600);
        }
    }

    #[test]
    fn test_env_var_ignored_when_config_has_value() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(
            &config_path,
            r#"
            [coord]
            url = "http://from-config:8080"
            admin_api_key = "config_key"
            [relay]
            url = "ws://127.0.0.1:7777"
            [mesh]
            listen = "0.0.0.0:8443"
            [storage]
            data_dir = "/var/lib/goy-node"
            [metrics]
            listen = "127.0.0.1:9090"
        "#,
        )
        .unwrap();

        // Definir env var diferente
        unsafe {
            std::env::set_var("GOY_API_URL", "http://from-env:9999");
        }

        let opts = ResolveOptions {
            config_path: Some(config_path.clone()),
            no_interactive: true,
            ..Default::default()
        };

        let resolved = resolve(&opts).unwrap();

        // Config file ganha — env var é ignorada
        assert_eq!(resolved.config.coord.url, "http://from-config:8080");
        assert_eq!(
            resolved.sources.get("coord.url"),
            Some(&ConfigSource::ConfigFile(config_path))
        );

        // Mas warning de deprecation foi emitido
        assert!(resolved.warnings.iter().any(|w| w.contains("deprecated")));

        unsafe {
            std::env::remove_var("GOY_API_URL");
        }
    }

    #[test]
    fn test_resolve_after_set_reads_updated_value() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("config.toml");

        // Criar config inicial
        let opts = ResolveOptions {
            config_path: Some(config_path.clone()),
            coord_url: Some("http://initial:8080".to_string()),
            admin_api_key: Some("test_key".to_string()),
            no_interactive: true,
            ..Default::default()
        };
        resolve(&opts).unwrap();

        // Modificar via set
        let mut config: GoyNodeConfig =
            toml::from_str(&std::fs::read_to_string(&config_path).unwrap()).unwrap();
        crate::config::commands::apply_set_field(&mut config, "coord.url", "http://updated:9090")
            .unwrap();
        crate::config::commands::write_config_auto(&config_path, &config).unwrap();

        // Resolver novamente — deve ler valor atualizado
        let opts2 = ResolveOptions {
            config_path: Some(config_path),
            no_interactive: true,
            ..Default::default()
        };
        let resolved = resolve(&opts2).unwrap();
        assert_eq!(resolved.config.coord.url, "http://updated:9090");
    }

    #[test]
    fn test_corrupt_config_gives_clear_error() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(&config_path, "this is not valid TOML {{{{").unwrap();

        let opts = ResolveOptions {
            config_path: Some(config_path.clone()),
            no_interactive: true,
            ..Default::default()
        };

        let result = resolve(&opts);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Failed to parse config"));
        assert!(err.contains(&config_path.display().to_string()));
    }

    #[test]
    #[cfg(unix)]
    fn test_warns_on_insecure_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let _lock = ENV_MUTEX.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(
            &config_path,
            r#"
            [coord]
            url = "http://localhost:8080"
            admin_api_key = "test_key"
            [relay]
            url = "ws://127.0.0.1:7777"
            [mesh]
            listen = "0.0.0.0:8443"
            [storage]
            data_dir = "/var/lib/goy-node"
            [metrics]
            listen = "127.0.0.1:9090"
        "#,
        )
        .unwrap();

        // Tornar world-readable
        std::fs::set_permissions(&config_path, std::fs::Permissions::from_mode(0o644)).unwrap();

        let opts = ResolveOptions {
            config_path: Some(config_path),
            no_interactive: true,
            ..Default::default()
        };

        let resolved = resolve(&opts).unwrap();
        assert!(resolved.warnings.iter().any(|w| {
            w.contains("insecure") || w.contains("permission") || w.contains("644")
        }));
    }
}
