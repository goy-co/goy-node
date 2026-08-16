#[cfg(test)]
mod tests {
    use crate::config::schema::*;
    use std::path::PathBuf;

    #[test]
    fn test_deserialize_full_config() {
        let toml_str = r#"
            [coord]
            url = "http://localhost:8080"
            admin_api_key = "test_key_123"
            heartbeat_interval_secs = 30
            request_timeout_secs = 10

            [relay]
            url = "ws://127.0.0.1:7777"

            [mesh]
            listen = "0.0.0.0:8443"
            seeds = ["ws://peer1:8443"]
            tls_enabled = true
            heartbeat_secs = 30

            [storage]
            data_dir = "/var/lib/goy-node"
            extra_contribution_gb = 10

            [metrics]
            listen = "127.0.0.1:9090"

            [log]
            level = "debug"
            format = "json"
        "#;

        let config: GoyNodeConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.coord.url, "http://localhost:8080");
        assert_eq!(config.storage.extra_contribution_gb, 10);
        assert_eq!(config.log.level, "debug");
        config.validate().unwrap();
    }

    #[test]
    fn test_deserialize_minimal_config() {
        // Só campos obrigatórios, resto usa defaults
        let toml_str = r#"
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
        "#;

        let config: GoyNodeConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.coord.heartbeat_interval_secs, 30); // default
        assert_eq!(config.log.level, "info"); // default
        config.validate().unwrap();
    }

    #[test]
    fn test_validate_rejects_empty_admin_key() {
        let mut config = valid_config();
        config.coord.admin_api_key = "".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_invalid_url() {
        let mut config = valid_config();
        config.coord.url = "not-a-url".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_relative_data_dir() {
        let mut config = valid_config();
        config.storage.data_dir = PathBuf::from("./relative");
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_invalid_log_level() {
        let mut config = valid_config();
        config.log.level = "verbose".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_roundtrip_serialize_deserialize() {
        let config = valid_config();
        let serialized = toml::to_string_pretty(&config).unwrap();
        let deserialized: GoyNodeConfig = toml::from_str(&serialized).unwrap();
        assert_eq!(config.coord.url, deserialized.coord.url);
        assert_eq!(config.mesh.seeds, deserialized.mesh.seeds);
    }

    fn valid_config() -> GoyNodeConfig {
        GoyNodeConfig {
            coord: CoordConfig {
                url: "http://localhost:8080".to_string(),
                admin_api_key: "test_key".to_string(),
                heartbeat_interval_secs: 30,
                request_timeout_secs: 10,
            },
            relay: RelayConfig {
                url: "ws://127.0.0.1:7777".to_string(),
                import_cmd: None,
            },
            mesh: MeshConfig {
                listen: "0.0.0.0:8443".to_string(),
                seeds: vec![],
                registry_url: None,
                heartbeat_secs: 30,
                tls_enabled: true,
                trusted_fingerprints: Default::default(),
            },
            storage: StorageConfig {
                data_dir: PathBuf::from("/var/lib/goy-node"),
                extra_contribution_gb: 0,
            },
            metrics: MetricsConfig {
                listen: "127.0.0.1:9090".to_string(),
            },
            log: LogConfig {
                level: "info".to_string(),
                format: "pretty".to_string(),
            },
        }
    }
}
