use super::schema::GoyNodeConfig;
use anyhow::{bail, Result};
use url::Url;

impl GoyNodeConfig {
    /// Valida semanticamente a configuração após deserialização.
    /// Chamado após load/merge, antes de usar qualquer valor.
    pub fn validate(&self) -> Result<()> {
        // 1. Coord URL deve ser válido
        Url::parse(&self.coord.url)
            .map_err(|e| anyhow::anyhow!("coord.url is invalid: {e}"))?;

        // 2. Admin API key não pode estar vazia
        if self.coord.admin_api_key.trim().is_empty() {
            bail!("coord.admin_api_key cannot be empty");
        }

        // 3. Heartbeat interval razoável (5s a 300s)
        if self.coord.heartbeat_interval_secs < 5 || self.coord.heartbeat_interval_secs > 300 {
            bail!(
                "coord.heartbeat_interval_secs must be between 5 and 300, got {}",
                self.coord.heartbeat_interval_secs
            );
        }

        // 4. Relay URL deve ser ws:// ou wss://
        if !self.relay.url.starts_with("ws://") && !self.relay.url.starts_with("wss://") {
            bail!(
                "relay.url must start with ws:// or wss://, got '{}'",
                self.relay.url
            );
        }

        // 5. Mesh listen deve ter formato addr:port
        if self.mesh.listen.parse::<std::net::SocketAddr>().is_err()
            && self.mesh.listen.parse::<std::net::Ipv4Addr>().is_err()
        {
            let parts: Vec<&str> = self.mesh.listen.split(':').collect();
            if parts.len() != 2 || parts[1].parse::<u16>().is_err() {
                bail!(
                    "mesh.listen must be in 'addr:port' format, got '{}'",
                    self.mesh.listen
                );
            }
        }

        // 6. Data dir deve ser path absoluto
        if !self.storage.data_dir.is_absolute() {
            bail!(
                "storage.data_dir must be an absolute path, got '{}'",
                self.storage.data_dir.display()
            );
        }

        // 7. Log level válido
        match self.log.level.as_str() {
            "trace" | "debug" | "info" | "warn" | "error" => {}
            other => {
                bail!("log.level must be one of: trace, debug, info, warn, error. Got '{other}'")
            }
        }

        // 8. Log format válido
        match self.log.format.as_str() {
            "pretty" | "json" => {}
            other => bail!("log.format must be 'pretty' or 'json'. Got '{other}'"),
        }

        // 9. Trusted fingerprints devem ser SHA-256 válidos (64 hex chars)
        for (url, fp) in &self.mesh.trusted_fingerprints {
            if fp.len() != 64 || !fp.chars().all(|c| c.is_ascii_hexdigit()) {
                bail!(
                    "mesh.trusted_fingerprints['{url}'] must be a 64-char hex SHA-256 hash, got '{fp}'"
                );
            }
        }

        Ok(())
    }
}
