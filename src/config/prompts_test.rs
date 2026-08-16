#[cfg(test)]
mod tests {
    use super::super::prompts::*;
    use super::super::resolver::{default_goy_node_config, ConfigSource, ResolveOptions};
    use super::super::schema::*;
    use std::collections::HashMap;

    fn valid_test_config() -> GoyNodeConfig {
        let mut cfg = default_goy_node_config();
        cfg.coord.url = "http://10.0.0.5:8080".to_string();
        cfg.coord.admin_api_key = "secret_key_12345".to_string();
        cfg
    }

    #[test]
    fn test_no_prompts_when_all_fields_present() {
        let mut config = valid_test_config();
        let opts = ResolveOptions::default();
        let mut sources = HashMap::new();
        sources.insert(
            "coord.url".to_string(),
            ConfigSource::ConfigFile("/test".into()),
        );
        sources.insert(
            "coord.admin_api_key".to_string(),
            ConfigSource::ConfigFile("/test".into()),
        );

        let result = prompt_missing_fields(&mut config, &opts, &sources).unwrap();
        assert!(!result.prompted);
        assert!(result.filled_fields.is_empty());
    }

    #[test]
    fn test_non_interactive_fails_on_missing() {
        let mut config = default_goy_node_config(); // url e key vazios/default
        let opts = ResolveOptions {
            no_interactive: true,
            ..Default::default()
        };
        let sources = HashMap::new();

        let result = prompt_missing_fields(&mut config, &opts, &sources);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("non-interactive"));
        assert!(err.contains("coord.url") || err.contains("coord.admin_api_key"));
    }

    #[test]
    fn test_non_interactive_succeeds_when_all_present() {
        let mut config = valid_test_config();
        let opts = ResolveOptions {
            no_interactive: true,
            ..Default::default()
        };
        let mut sources = HashMap::new();
        sources.insert(
            "coord.url".to_string(),
            ConfigSource::CliFlag("coord-url".into()),
        );
        sources.insert(
            "coord.admin_api_key".to_string(),
            ConfigSource::CliFlag("admin-api-key".into()),
        );

        let result = prompt_missing_fields(&mut config, &opts, &sources).unwrap();
        assert!(!result.prompted);
        assert!(result.filled_fields.is_empty());
    }

    #[test]
    fn test_only_prompts_missing_fields() {
        let mut config = valid_test_config();
        config.coord.admin_api_key = String::new(); // Só key falta
        let mut sources = HashMap::new();
        sources.insert(
            "coord.url".to_string(),
            ConfigSource::ConfigFile("/test".into()),
        );
        // admin_api_key sem source → considerado faltante

        let missing_key = config.coord.admin_api_key.trim().is_empty()
            || is_default_unconfigured(&sources, "coord.admin_api_key");
        assert!(missing_key);

        let missing_url = config.coord.url.is_empty()
            || (config.coord.url == "http://localhost:8080"
                && is_default_unconfigured(&sources, "coord.url"));
        assert!(!missing_url); // URL está presente e configurada no ficheiro
    }

    #[test]
    fn test_auto_generate_with_partial_cli_flags_triggers_prompts_for_rest() {
        let mut config = default_goy_node_config();
        // Simular que coord_url veio de flag CLI mas admin_api_key não
        config.coord.url = "http://10.0.0.5:8080".to_string();
        let mut sources = HashMap::new();
        sources.insert(
            "coord.url".to_string(),
            ConfigSource::CliFlag("coord-url".to_string()),
        );

        let opts = ResolveOptions {
            no_interactive: true,
            ..Default::default()
        };

        // Em non-interactive, deve falhar acusando especificamente admin_api_key e NÃO coord.url
        let result = prompt_missing_fields(&mut config, &opts, &sources);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("coord.admin_api_key"));
        assert!(!err.contains("coord.url"));
    }
}
