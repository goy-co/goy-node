//! Testes de integração de TLS mútuo entre peers do mesh.
//!
//! Cobre:
//! - Dois nós com TLS → conexão bem-sucedida com verificação de fingerprint
//! - Fingerprint errado (pinned) → conexão rejeitada
//! - TOFU: primeira conexão aceite, segunda com fingerprint diferente rejeitada
//! - Fallback plaintext com `tls_enabled = false`

use std::collections::HashMap;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use goy_node::config::MeshConfig;
use goy_node::relay::RelayEvent;
use goy_node::tls::{self, FingerprintStore, TrustDecision};

const EVENT_A: &str = r#"["EVENT","sub_1",{"id":"e000000000000000000000000000000000000000000000000000000000000001","content":"A to B"}]"#;
const EVENT_A_OUT: &str = r#"["EVENT",{"id":"e000000000000000000000000000000000000000000000000000000000000001","content":"A to B"}]"#;

/// Reserva uma porta livre em loopback.
async fn free_addr() -> anyhow::Result<std::net::SocketAddr> {
    let l = TcpListener::bind("127.0.0.1:0").await?;
    let addr = l.local_addr()?;
    drop(l);
    Ok(addr)
}

/// Sobe um nó mesh e devolve o canal de eventos locais + o canal de publicação.
fn spawn_node(
    cfg: MeshConfig,
    data_dir: Option<std::path::PathBuf>,
    cancel: CancellationToken,
) -> (broadcast::Sender<RelayEvent>, mpsc::Receiver<String>) {
    let (events_tx, events_rx) = broadcast::channel::<RelayEvent>(16);
    let (publish_tx, publish_rx) = mpsc::channel::<String>(16);
    tokio::spawn(async move {
        let _ = goy_node::mesh::run(
            cfg,
            "ws://127.0.0.1:57777".to_string(),
            data_dir,
            events_rx,
            publish_tx,
            cancel,
        )
        .await;
    });
    (events_tx, publish_rx)
}

/// Dois nós com TLS ativo estabelecem a conexão e sincronizam eventos.
/// O fingerprint de cada peer é aprendido e persistido em `known_fingerprints.json`.
#[tokio::test]
async fn test_two_nodes_tls_connect_and_sync() -> anyhow::Result<()> {
    let cancel = CancellationToken::new();
    let addr_a = free_addr().await?;
    let addr_b = free_addr().await?;

    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;

    let cfg_a = MeshConfig {
        listen: addr_a.to_string(),
        seeds: vec![],
        tls_enabled: true,
        ..MeshConfig::default()
    };
    let cfg_b = MeshConfig {
        listen: addr_b.to_string(),
        seeds: vec![format!("ws://{addr_a}")],
        tls_enabled: true,
        ..MeshConfig::default()
    };

    let (events_a, _rx_a) = spawn_node(cfg_a, Some(dir_a.path().to_path_buf()), cancel.clone());
    let (_events_b, mut rx_b) = spawn_node(cfg_b, Some(dir_b.path().to_path_buf()), cancel.clone());

    tokio::time::sleep(Duration::from_millis(600)).await;

    // O evento chega ao nó B através do túnel TLS.
    events_a.send(RelayEvent {
        raw: EVENT_A.to_string(),
    })?;
    let received = tokio::time::timeout(Duration::from_secs(3), rx_b.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Node B did not receive the event over TLS"))?;
    assert_eq!(received, EVENT_A_OUT);

    // Ambos os nós geraram e persistiram o seu certificado.
    for dir in [dir_a.path(), dir_b.path()] {
        assert!(
            dir.join("tls/node_cert.pem").exists(),
            "cert missing in {dir:?}"
        );
        assert!(
            dir.join("tls/node_key.pem").exists(),
            "key missing in {dir:?}"
        );
    }

    // O nó B aprendeu (TOFU) e persistiu o fingerprint do nó A.
    let known_b = dir_b.path().join("known_fingerprints.json");
    assert!(
        known_b.exists(),
        "node B did not persist known fingerprints"
    );
    let map: HashMap<String, String> = serde_json::from_slice(&std::fs::read(&known_b)?)?;
    let peer_key = format!("ws://{addr_a}");
    let learned = map
        .get(&peer_key)
        .unwrap_or_else(|| panic!("no fingerprint learned for {peer_key}, got {map:?}"));

    // E esse fingerprint é exatamente o do certificado do nó A.
    let cert_a = tls::load_or_generate_cert(dir_a.path(), "unused-already-on-disk")?;
    assert_eq!(learned, &cert_a.fingerprint);

    cancel.cancel();
    Ok(())
}

/// Um fingerprint pré-aprovado errado faz a conexão outbound ser rejeitada:
/// nenhum evento atravessa.
#[tokio::test]
async fn test_wrong_pinned_fingerprint_rejects_connection() -> anyhow::Result<()> {
    let cancel = CancellationToken::new();
    let addr_a = free_addr().await?;
    let addr_b = free_addr().await?;

    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;

    // O nó B espera um fingerprint que o nó A nunca terá.
    let mut pinned = HashMap::new();
    pinned.insert(format!("ws://{addr_a}"), "aa".repeat(32));

    let cfg_a = MeshConfig {
        listen: addr_a.to_string(),
        tls_enabled: true,
        ..MeshConfig::default()
    };
    let cfg_b = MeshConfig {
        listen: addr_b.to_string(),
        seeds: vec![format!("ws://{addr_a}")],
        tls_enabled: true,
        trusted_fingerprints: pinned,
        ..MeshConfig::default()
    };

    let (events_a, _rx_a) = spawn_node(cfg_a, Some(dir_a.path().to_path_buf()), cancel.clone());
    let (_events_b, mut rx_b) = spawn_node(cfg_b, Some(dir_b.path().to_path_buf()), cancel.clone());

    tokio::time::sleep(Duration::from_millis(800)).await;

    events_a.send(RelayEvent {
        raw: EVENT_A.to_string(),
    })?;

    // O handshake é rejeitado, logo nada chega ao nó B.
    let result = tokio::time::timeout(Duration::from_millis(1200), rx_b.recv()).await;
    assert!(
        result.is_err(),
        "event unexpectedly delivered despite fingerprint mismatch: {result:?}"
    );

    cancel.cancel();
    Ok(())
}

/// TOFU no store persistido: a primeira conexão aprende o fingerprint e a
/// segunda, com um fingerprint diferente para o mesmo peer, é rejeitada.
#[tokio::test]
async fn test_tofu_first_use_accepted_then_changed_rejected() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let peer = "ws://peer-a:8443";

    let cert_original = tls::generate_self_signed("peer-a")?;
    let cert_impostor = tls::generate_self_signed("peer-a")?;
    assert_ne!(cert_original.fingerprint, cert_impostor.fingerprint);

    // Primeira conexão: peer desconhecido → aceite e guardado.
    {
        let store = FingerprintStore::load(Some(dir.path()), &HashMap::new());
        assert_eq!(
            store.verify_or_learn(peer, &cert_original.fingerprint),
            TrustDecision::LearnedOnFirstUse
        );
    }

    // Reinício do nó: o store é recarregado do disco.
    let store = FingerprintStore::load(Some(dir.path()), &HashMap::new());

    // Mesmo certificado → aceite.
    assert_eq!(
        store.verify_or_learn(peer, &cert_original.fingerprint),
        TrustDecision::Match
    );

    // Certificado diferente para o mesmo peer → rejeitado (possível MITM).
    match store.verify_or_learn(peer, &cert_impostor.fingerprint) {
        TrustDecision::Mismatch { expected, received } => {
            assert_eq!(expected, cert_original.fingerprint);
            assert_eq!(received, cert_impostor.fingerprint);
        }
        other => panic!("expected Mismatch for rotated certificate, got {other:?}"),
    }

    Ok(())
}

/// Com `tls_enabled = false` os nós falam TCP plaintext e continuam a
/// sincronizar; nenhum certificado é gerado em disco.
#[tokio::test]
async fn test_plaintext_fallback_when_tls_disabled() -> anyhow::Result<()> {
    let cancel = CancellationToken::new();
    let addr_a = free_addr().await?;
    let addr_b = free_addr().await?;

    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;

    let cfg_a = MeshConfig {
        listen: addr_a.to_string(),
        tls_enabled: false,
        ..MeshConfig::default()
    };
    let cfg_b = MeshConfig {
        listen: addr_b.to_string(),
        seeds: vec![format!("ws://{addr_a}")],
        tls_enabled: false,
        ..MeshConfig::default()
    };

    let (events_a, _rx_a) = spawn_node(cfg_a, Some(dir_a.path().to_path_buf()), cancel.clone());
    let (_events_b, mut rx_b) = spawn_node(cfg_b, Some(dir_b.path().to_path_buf()), cancel.clone());

    tokio::time::sleep(Duration::from_millis(500)).await;

    events_a.send(RelayEvent {
        raw: EVENT_A.to_string(),
    })?;
    let received = tokio::time::timeout(Duration::from_secs(3), rx_b.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Node B did not receive the event over plaintext"))?;
    assert_eq!(received, EVENT_A_OUT);

    // Sem TLS não há certificados nem store de fingerprints.
    assert!(!dir_a.path().join("tls").exists());
    assert!(!dir_b.path().join("known_fingerprints.json").exists());

    cancel.cancel();
    Ok(())
}

/// Um fingerprint mal formado na config é rejeitado na validação.
#[test]
fn test_config_rejects_malformed_trusted_fingerprint() {
    let toml = r#"
[relay]
url = "ws://127.0.0.1:7777"

[mesh]
listen = "0.0.0.0:8443"

[mesh.trusted_fingerprints]
"ws://peer1:8443" = "not-a-fingerprint"
"#;
    let err = goy_node::config::Config::load_from_str(toml)
        .expect_err("malformed fingerprint should fail validation");
    assert!(
        err.to_string().contains("not a 64-char SHA-256 hex digest"),
        "unexpected error: {err}"
    );
}

/// Um fingerprint válido em qualquer notação comum é aceite e normalizado.
#[test]
fn test_config_accepts_colon_separated_fingerprint() -> anyhow::Result<()> {
    let raw = "ab".repeat(32);
    let colonized: Vec<String> = raw
        .as_bytes()
        .chunks(2)
        .map(|c| String::from_utf8_lossy(c).to_uppercase())
        .collect();
    let toml = format!(
        r#"
[relay]
url = "ws://127.0.0.1:7777"

[mesh]
listen = "0.0.0.0:8443"

[mesh.trusted_fingerprints]
"ws://peer1:8443" = "{}"
"#,
        colonized.join(":")
    );

    let cfg = goy_node::config::Config::load_from_str(&toml)?;
    let stored = &cfg.mesh.trusted_fingerprints["ws://peer1:8443"];
    assert_eq!(tls::normalize_fingerprint(stored), raw);
    Ok(())
}
