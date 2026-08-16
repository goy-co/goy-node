use super::resolver::{ConfigSource, ResolveOptions};
use super::schema::GoyNodeConfig;
use anyhow::{bail, Result};
use std::collections::HashMap;
use std::io::IsTerminal;

/// Resultado dos prompts interativos.
#[derive(Debug, Default)]
pub struct PromptResult {
    /// Campos preenchidos via prompt.
    pub filled_fields: HashMap<String, String>,
    /// Se algum prompt foi feito.
    pub prompted: bool,
}

/// Executa prompts interativos para campos obrigatórios faltantes.
/// Retorna Ok(PromptResult) se todos os campos foram preenchidos ou se não havia nada em falta.
/// Retorna Err se --no-interactive está ativo e falta algo ou se o ambiente não é um terminal TTY.
pub fn prompt_missing_fields(
    config: &mut GoyNodeConfig,
    opts: &ResolveOptions,
    sources: &HashMap<String, ConfigSource>,
) -> Result<PromptResult> {
    let mut result = PromptResult::default();

    // Identificar campos obrigatórios faltantes
    let missing_url = config.coord.url.is_empty()
        || (config.coord.url == "http://localhost:8080"
            && is_default_unconfigured(sources, "coord.url"));
    let missing_key = config.coord.admin_api_key.trim().is_empty()
        || is_default_unconfigured(sources, "coord.admin_api_key");

    if !missing_url && !missing_key {
        return Ok(result);
    }

    // Verificar se podemos fazer prompts
    if opts.no_interactive {
        let mut missing = Vec::new();
        if missing_url {
            missing.push("coord.url (--coord-url)");
        }
        if missing_key {
            missing.push("coord.admin_api_key (--admin-api-key)");
        }
        bail!(
            "Missing required configuration in non-interactive mode:\n\
             • {}\n\n\
             Provide these via CLI flags or remove --no-interactive for interactive setup.\n\
             Example: goy-node --coord-url http://host:8080 --admin-api-key KEY onboard --auth-key gc_...",
            missing.join("\n• ")
        );
    }

    if !std::io::stdout().is_terminal() {
        bail!(
            "Missing required configuration and stdout is not a terminal.\n\
             Cannot prompt interactively. Provide via CLI flags:\n\
             --coord-url <URL> --admin-api-key <KEY>"
        );
    }

    // ── Fazer prompts ──────────────────────────────────────────────────
    println!();
    println!("🔧 First-time setup — some configuration is missing.");
    println!("   Press Enter to accept defaults shown in [brackets].");
    println!();

    if missing_url {
        let url = prompt_text("Coord-server URL", "http://localhost:8080")?;
        config.coord.url = url.clone();
        result.filled_fields.insert("coord.url".to_string(), url);
        result.prompted = true;
    }

    if missing_key {
        let key = prompt_secret("Admin API Key")?;
        if key.trim().is_empty() {
            bail!("Admin API Key cannot be empty.");
        }
        config.coord.admin_api_key = key.clone();
        result
            .filled_fields
            .insert("coord.admin_api_key".to_string(), key);
        result.prompted = true;
    }

    // Prompts opcionais (só se o utilizador já está em modo interativo)
    if result.prompted {
        let data_dir_str = config.storage.data_dir.display().to_string();
        let data_dir = prompt_text("Data directory", &data_dir_str)?;
        if data_dir != data_dir_str {
            config.storage.data_dir = std::path::PathBuf::from(&data_dir);
            result
                .filled_fields
                .insert("storage.data_dir".to_string(), data_dir);
        }

        let relay_url = prompt_text("Relay URL", &config.relay.url)?;
        if relay_url != config.relay.url {
            config.relay.url = relay_url.clone();
            result
                .filled_fields
                .insert("relay.url".to_string(), relay_url);
        }

        let mesh_listen = prompt_text("Mesh listen address", &config.mesh.listen)?;
        if mesh_listen != config.mesh.listen {
            config.mesh.listen = mesh_listen.clone();
            result
                .filled_fields
                .insert("mesh.listen".to_string(), mesh_listen);
        }
    }

    println!();
    Ok(result)
}

pub fn is_default_unconfigured(sources: &HashMap<String, ConfigSource>, field: &str) -> bool {
    matches!(sources.get(field), Some(ConfigSource::Default) | None)
}

/// Prompt de texto com default.
fn prompt_text(prompt: &str, default: &str) -> Result<String> {
    use dialoguer::Input;

    let value: String = Input::new()
        .with_prompt(format!("{prompt} [{default}]"))
        .default(default.to_string())
        .allow_empty(false)
        .interact_text()?;

    Ok(value)
}

/// Prompt de secret (input escondido, sem echo).
fn prompt_secret(prompt: &str) -> Result<String> {
    use rpassword::prompt_password;

    let value = prompt_password(format!("{prompt}: "))?;
    Ok(value)
}
