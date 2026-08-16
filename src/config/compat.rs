use super::schema::GoyNodeConfig;
use super::{Config, HeartbeatConfig, MeshConfig, MetricsConfig, RelayConfig};

impl From<GoyNodeConfig> for Config {
    fn from(new: GoyNodeConfig) -> Self {
        Self {
            relay: RelayConfig {
                url: new.relay.url,
                import_cmd: new.relay.import_cmd,
            },
            mesh: MeshConfig {
                listen: new.mesh.listen,
                seeds: new.mesh.seeds,
                registry_url: new.mesh.registry_url,
                heartbeat_secs: new.mesh.heartbeat_secs,
                discovery_secs: 60,
                mesh_url: None,
                node_id: None,
                replication_factor: 3,
                vnodes_per_peer: 150,
                max_events_per_second_per_peer: 50,
                max_bytes_per_second_per_peer: 1_048_576,
                max_message_size: 524_288,
                tls_enabled: new.mesh.tls_enabled,
                trusted_fingerprints: new.mesh.trusted_fingerprints,
            },
            metrics: MetricsConfig {
                listen: if new.metrics.listen.is_empty() || new.metrics.listen == "off" {
                    None
                } else {
                    Some(new.metrics.listen)
                },
            },
            storage: crate::storage::StorageConfig {
                data_dir: new.storage.data_dir,
                extra_contribution_gb: new.storage.extra_contribution_gb,
            },
            heartbeat: HeartbeatConfig {
                enabled: true,
                interval_secs: new.coord.heartbeat_interval_secs,
            },
        }
    }
}
