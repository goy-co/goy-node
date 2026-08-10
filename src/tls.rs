//! TLS mútuo entre peers do mesh.
//!
//! - Geração/carregamento de certificado auto-assinado por nó (`data_dir/tls/`)
//! - Fingerprint SHA-256 do certificado como identidade do nó
//! - Trust-on-first-use (TOFU) persistido em `data_dir/known_fingerprints.json`
//! - Configurações rustls (TLS 1.3 apenas) para listener e conexões outbound
//!
//! A confiança **não** é baseada numa CA pública: qualquer certificado
//! estruturalmente válido é aceite, e a identidade é verificada através do
//! fingerprint (pré-aprovado por config ou aprendido na primeira conexão).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{ClientConfig, DigitallySignedStruct, DistinguishedName, ServerConfig, SignatureScheme};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

/// Validade do certificado auto-assinado: 10 anos (não há CA para renovar).
const CERT_VALIDITY_DAYS: i64 = 3650;

/// Certificado e chave privada do nó local, com o respetivo fingerprint.
#[derive(Debug, Clone)]
pub struct NodeCertificate {
    /// Certificado em DER.
    pub cert_der: Vec<u8>,
    /// Chave privada PKCS#8 em DER.
    pub key_der: Vec<u8>,
    /// Fingerprint SHA-256 do certificado (hex minúsculo, 64 chars).
    pub fingerprint: String,
}

impl NodeCertificate {
    /// Certificado no formato rustls.
    pub fn certificate_der(&self) -> CertificateDer<'static> {
        CertificateDer::from(self.cert_der.clone())
    }

    /// Chave privada no formato rustls.
    pub fn private_key_der(&self) -> PrivateKeyDer<'static> {
        PrivateKeyDer::try_from(self.key_der.clone())
            .expect("private key DER generated/loaded by this module is always valid PKCS#8")
    }
}

/// Calcula o fingerprint SHA-256 (hex minúsculo) de um certificado DER.
pub fn fingerprint_der(cert_der: &[u8]) -> String {
    let digest = Sha256::digest(cert_der);
    hex::encode(digest)
}

/// Normaliza um fingerprint escrito por humanos: aceita `AA:BB:CC…`, maiúsculas
/// e o prefixo `sha256:`.
pub fn normalize_fingerprint(fp: &str) -> String {
    fp.trim()
        .trim_start_matches("sha256:")
        .trim_start_matches("SHA256:")
        .replace([':', ' ', '-'], "")
        .to_ascii_lowercase()
}

/// Diretório onde vivem o certificado e a chave do nó.
pub fn tls_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("tls")
}

/// Carrega o certificado do nó de `data_dir/tls/`, ou gera um novo
/// auto-assinado se ainda não existir.
///
/// O certificado inclui o `node_id` como SAN (DNS name) e é válido por 10 anos.
pub fn load_or_generate_cert(data_dir: &Path, node_id: &str) -> anyhow::Result<NodeCertificate> {
    let dir = tls_dir(data_dir);
    let cert_path = dir.join("node_cert.pem");
    let key_path = dir.join("node_key.pem");

    if cert_path.exists() && key_path.exists() {
        match load_cert_from_disk(&cert_path, &key_path) {
            Ok(cert) => {
                info!("🔐 Loaded existing node certificate from {}", dir.display());
                return Ok(cert);
            }
            Err(e) => {
                warn!(
                    "⚠️  Failed to load existing certificate from {}: {e}. Regenerating.",
                    dir.display()
                );
            }
        }
    }

    let cert = generate_self_signed(node_id)?;
    std::fs::create_dir_all(&dir)?;

    let cert_pem = pem_encode("CERTIFICATE", &cert.cert_der);
    let key_pem = pem_encode("PRIVATE KEY", &cert.key_der);

    write_atomic(&cert_path, cert_pem.as_bytes())?;
    write_atomic(&key_path, key_pem.as_bytes())?;
    restrict_permissions(&key_path);

    info!("🔐 Generated new self-signed node certificate at {}", dir.display());
    Ok(cert)
}

/// Gera um certificado auto-assinado novo para `node_id` (não escreve em disco).
pub fn generate_self_signed(node_id: &str) -> anyhow::Result<NodeCertificate> {
    let mut params = rcgen::CertificateParams::new(vec![node_id.to_string()])
        .map_err(|e| anyhow::anyhow!("invalid SAN for node_id '{node_id}': {e}"))?;

    params.distinguished_name = {
        let mut dn = rcgen::DistinguishedName::new();
        dn.push(rcgen::DnType::CommonName, node_id);
        dn.push(rcgen::DnType::OrganizationName, "The Goy Company");
        dn
    };
    let (not_before, not_after) = validity_window();
    params.not_before = not_before;
    params.not_after = not_after;

    let key_pair = rcgen::KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;

    let cert_der = cert.der().to_vec();
    let key_der = key_pair.serialize_der();
    let fingerprint = fingerprint_der(&cert_der);

    Ok(NodeCertificate {
        cert_der,
        key_der,
        fingerprint,
    })
}

/// Janela de validade do certificado: de agora até 10 anos no futuro.
fn validity_window() -> (time::OffsetDateTime, time::OffsetDateTime) {
    let now = time::OffsetDateTime::now_utc();
    (now, now + time::Duration::days(CERT_VALIDITY_DAYS))
}

fn load_cert_from_disk(cert_path: &Path, key_path: &Path) -> anyhow::Result<NodeCertificate> {
    let cert_bytes = std::fs::read(cert_path)?;
    let key_bytes = std::fs::read(key_path)?;

    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut cert_bytes.as_slice()).collect::<Result<_, _>>()?;
    let cert_der = certs
        .first()
        .ok_or_else(|| anyhow::anyhow!("no certificate found in {}", cert_path.display()))?
        .to_vec();

    let key = rustls_pemfile::private_key(&mut key_bytes.as_slice())?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {}", key_path.display()))?;
    let key_der = key.secret_der().to_vec();

    let fingerprint = fingerprint_der(&cert_der);
    Ok(NodeCertificate {
        cert_der,
        key_der,
        fingerprint,
    })
}

fn pem_encode(label: &str, der: &[u8]) -> String {
    use std::fmt::Write;
    let b64 = base64_encode(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        let _ = writeln!(out, "{}", std::str::from_utf8(chunk).unwrap_or_default());
    }
    let _ = writeln!(out, "-----END {label}-----");
    out
}

/// Base64 standard (RFC 4648) sem dependência externa.
fn base64_encode(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[(n >> 18) as usize & 63] as char);
        out.push(TABLE[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

fn write_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(unix)]
fn restrict_permissions(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Err(e) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
        warn!("⚠️  Failed to restrict permissions on {}: {e}", path.display());
    }
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &Path) {}

// ─────────────────────────── TOFU fingerprint store ───────────────────────────

/// Armazena os fingerprints conhecidos por peer (trust-on-first-use), com
/// persistência em `data_dir/known_fingerprints.json` e overrides pré-aprovados
/// vindos da configuração.
#[derive(Debug)]
pub struct FingerprintStore {
    path: Option<PathBuf>,
    /// Fingerprints pré-aprovados via config (nunca sobrescritos por TOFU).
    pinned: HashMap<String, String>,
    /// Fingerprints aprendidos (peer_id -> fingerprint).
    known: Mutex<HashMap<String, String>>,
}

/// Resultado da verificação de um fingerprint contra o store.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum TrustDecision {
    /// Fingerprint bate com o valor previamente conhecido/pinned.
    Match,
    /// Peer desconhecido: aceite e guardado (trust-on-first-use).
    LearnedOnFirstUse,
    /// Fingerprint diferente do esperado — possível MITM.
    Mismatch { expected: String, received: String },
}

impl FingerprintStore {
    /// Cria um store carregando `data_dir/known_fingerprints.json` (se existir)
    /// e aplicando os fingerprints pré-aprovados da config.
    pub fn load(data_dir: Option<&Path>, pinned: &HashMap<String, String>) -> Self {
        let path = data_dir.map(|d| d.join("known_fingerprints.json"));
        let mut known: HashMap<String, String> = HashMap::new();

        if let Some(ref p) = path {
            if p.exists() {
                match std::fs::read(p).map_err(anyhow::Error::from).and_then(|b| {
                    serde_json::from_slice::<HashMap<String, String>>(&b).map_err(Into::into)
                }) {
                    Ok(map) => {
                        info!("🔐 Loaded {} known peer fingerprints from {}", map.len(), p.display());
                        known = map
                            .into_iter()
                            .map(|(k, v)| (k, normalize_fingerprint(&v)))
                            .collect();
                    }
                    Err(e) => {
                        warn!("⚠️  Failed to load {}: {e}. Starting with empty TOFU store.", p.display());
                    }
                }
            }
        }

        let pinned: HashMap<String, String> = pinned
            .iter()
            .map(|(k, v)| (k.clone(), normalize_fingerprint(v)))
            .collect();

        if !pinned.is_empty() {
            info!("🔐 {} pre-approved peer fingerprints configured", pinned.len());
        }

        Self {
            path,
            pinned,
            known: Mutex::new(known),
        }
    }

    /// Store em memória, sem persistência (testes / `data_dir` ausente).
    #[allow(dead_code)]
    pub fn in_memory() -> Self {
        Self {
            path: None,
            pinned: HashMap::new(),
            known: Mutex::new(HashMap::new()),
        }
    }

    /// Fingerprint esperado para um peer (pinned tem prioridade sobre TOFU).
    pub fn expected(&self, peer_id: &str) -> Option<String> {
        if let Some(fp) = self.pinned.get(peer_id) {
            return Some(fp.clone());
        }
        self.known.lock().ok()?.get(peer_id).cloned()
    }

    /// Verifica o fingerprint apresentado por `peer_id`, aprendendo-o na
    /// primeira conexão (TOFU) e persistindo em disco.
    pub fn verify_or_learn(&self, peer_id: &str, received: &str) -> TrustDecision {
        let received = normalize_fingerprint(received);

        if let Some(expected) = self.expected(peer_id) {
            return if expected == received {
                TrustDecision::Match
            } else {
                TrustDecision::Mismatch { expected, received }
            };
        }

        if let Ok(mut known) = self.known.lock() {
            known.insert(peer_id.to_string(), received.clone());
        }
        self.persist();
        info!("🔐 TOFU: learned fingerprint {received} for new peer {peer_id}");
        TrustDecision::LearnedOnFirstUse
    }

    /// Escreve o mapa de fingerprints conhecidos em disco (atómico).
    pub fn persist(&self) {
        let Some(ref path) = self.path else { return };
        let Ok(known) = self.known.lock() else { return };

        if let Some(parent) = path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                warn!("⚠️  Failed to create {}: {e}", parent.display());
                return;
            }
        }
        if let Ok(bytes) = serde_json::to_vec_pretty(&*known) {
            if let Err(e) = write_atomic(path, &bytes) {
                warn!("⚠️  Failed to persist known fingerprints to {}: {e}", path.display());
            }
        }
    }
}

// ───────────────────────────── rustls configuration ─────────────────────────────

/// Verificador de certificados de cliente que aceita qualquer certificado
/// estruturalmente válido. A confiança real é feita depois do handshake, com
/// base no fingerprint (ver [`FingerprintStore`]).
#[derive(Debug)]
struct AcceptAnyClientCert {
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ClientCertVerifier for AcceptAnyClientCert {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Verificador de certificado de servidor baseado em fingerprint.
///
/// - Se `expected` for `Some`, o fingerprint tem de bater exatamente.
/// - Se for `None` (peer novo), aceita e regista o fingerprint observado em
///   `observed` para o chamador aplicar TOFU depois do handshake.
#[derive(Debug)]
struct FingerprintServerVerifier {
    provider: Arc<rustls::crypto::CryptoProvider>,
    expected: Option<String>,
    observed: Arc<Mutex<Option<String>>>,
    peer_id: String,
}

impl ServerCertVerifier for FingerprintServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let received = fingerprint_der(end_entity.as_ref());

        if let Ok(mut slot) = self.observed.lock() {
            *slot = Some(received.clone());
        }

        match self.expected {
            Some(ref expected) if *expected != received => {
                warn!(
                    "🚨 TLS fingerprint mismatch for peer {}: expected {expected}, received {received} (possible MITM)",
                    self.peer_id
                );
                Err(rustls::Error::General(format!(
                    "certificate fingerprint mismatch for {}: expected {expected}, received {received}",
                    self.peer_id
                )))
            }
            _ => Ok(ServerCertVerified::assertion()),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Configuração TLS 1.3 do listener (mTLS: exige certificado do cliente).
pub fn server_config(cert: &NodeCertificate) -> anyhow::Result<Arc<ServerConfig>> {
    let provider = provider();
    let verifier = Arc::new(AcceptAnyClientCert {
        provider: provider.clone(),
    });

    let config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![cert.certificate_der()], cert.private_key_der())?;

    Ok(Arc::new(config))
}

/// Configuração TLS 1.3 de cliente para conectar a `peer_id`.
///
/// Devolve também o slot onde o fingerprint observado do servidor é registado
/// durante o handshake (para aplicar TOFU depois).
pub fn client_config(
    cert: &NodeCertificate,
    peer_id: &str,
    expected_fingerprint: Option<String>,
) -> anyhow::Result<(Arc<ClientConfig>, Arc<Mutex<Option<String>>>)> {
    let provider = provider();
    let observed = Arc::new(Mutex::new(None));

    let verifier = Arc::new(FingerprintServerVerifier {
        provider: provider.clone(),
        expected: expected_fingerprint.map(|f| normalize_fingerprint(&f)),
        observed: observed.clone(),
        peer_id: peer_id.to_string(),
    });

    let config = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_auth_cert(vec![cert.certificate_der()], cert.private_key_der())?;

    Ok((Arc::new(config), observed))
}

/// Extrai o fingerprint do certificado apresentado pelo peer numa conexão TLS já estabelecida.
pub fn peer_fingerprint(conn: &rustls::CommonState) -> Option<String> {
    conn.peer_certificates()
        .and_then(|certs| certs.first())
        .map(|c| fingerprint_der(c.as_ref()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_and_load_certificate_roundtrip() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let node_id = "node-alpha";

        let first = load_or_generate_cert(dir.path(), node_id)?;
        assert!(tls_dir(dir.path()).join("node_cert.pem").exists());
        assert!(tls_dir(dir.path()).join("node_key.pem").exists());
        assert_eq!(first.fingerprint.len(), 64);

        // Segunda chamada carrega em vez de regenerar → mesmo fingerprint.
        let second = load_or_generate_cert(dir.path(), node_id)?;
        assert_eq!(first.fingerprint, second.fingerprint);
        assert_eq!(first.cert_der, second.cert_der);
        Ok(())
    }

    #[test]
    fn test_distinct_nodes_get_distinct_fingerprints() -> anyhow::Result<()> {
        let a = generate_self_signed("node-a")?;
        let b = generate_self_signed("node-b")?;
        assert_ne!(a.fingerprint, b.fingerprint);
        Ok(())
    }

    #[test]
    fn test_fingerprint_is_sha256_hex_of_der() -> anyhow::Result<()> {
        let cert = generate_self_signed("node-fp")?;
        let expected = hex::encode(Sha256::digest(&cert.cert_der));
        assert_eq!(cert.fingerprint, expected);
        assert_eq!(cert.fingerprint, fingerprint_der(&cert.cert_der));
        assert!(cert.fingerprint.chars().all(|c| c.is_ascii_hexdigit()));
        Ok(())
    }

    #[test]
    fn test_normalize_fingerprint_forms() {
        let canonical = "aabbcc";
        assert_eq!(normalize_fingerprint("AA:BB:CC"), canonical);
        assert_eq!(normalize_fingerprint("sha256:AABBCC"), canonical);
        assert_eq!(normalize_fingerprint("  aa bb cc "), canonical);
    }

    #[test]
    fn test_tofu_learns_then_matches_then_rejects() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let store = FingerprintStore::load(Some(dir.path()), &HashMap::new());

        assert_eq!(
            store.verify_or_learn("peer-1", "aa11"),
            TrustDecision::LearnedOnFirstUse
        );
        assert_eq!(store.verify_or_learn("peer-1", "aa11"), TrustDecision::Match);
        assert_eq!(
            store.verify_or_learn("peer-1", "bb22"),
            TrustDecision::Mismatch {
                expected: "aa11".into(),
                received: "bb22".into()
            }
        );
        Ok(())
    }

    #[test]
    fn test_tofu_persists_across_reload() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        {
            let store = FingerprintStore::load(Some(dir.path()), &HashMap::new());
            store.verify_or_learn("peer-x", "DEADBEEF");
        }
        assert!(dir.path().join("known_fingerprints.json").exists());

        let reloaded = FingerprintStore::load(Some(dir.path()), &HashMap::new());
        assert_eq!(reloaded.expected("peer-x").as_deref(), Some("deadbeef"));
        assert_eq!(reloaded.verify_or_learn("peer-x", "deadbeef"), TrustDecision::Match);
        Ok(())
    }

    #[test]
    fn test_pinned_fingerprint_overrides_tofu() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut pinned = HashMap::new();
        pinned.insert("peer-pin".to_string(), "AB:CD".to_string());

        let store = FingerprintStore::load(Some(dir.path()), &pinned);
        assert_eq!(store.expected("peer-pin").as_deref(), Some("abcd"));
        assert_eq!(store.verify_or_learn("peer-pin", "abcd"), TrustDecision::Match);
        assert!(matches!(
            store.verify_or_learn("peer-pin", "9999"),
            TrustDecision::Mismatch { .. }
        ));
        Ok(())
    }

    #[test]
    fn test_server_and_client_configs_build() -> anyhow::Result<()> {
        let cert = generate_self_signed("node-cfg")?;
        assert!(server_config(&cert).is_ok());
        let (_cfg, observed) = client_config(&cert, "peer", Some(cert.fingerprint.clone()))?;
        assert!(observed.lock().unwrap().is_none());
        Ok(())
    }

    #[test]
    fn test_pem_encoding_is_parseable() -> anyhow::Result<()> {
        let cert = generate_self_signed("node-pem")?;
        let pem = pem_encode("CERTIFICATE", &cert.cert_der);
        assert!(pem.starts_with("-----BEGIN CERTIFICATE-----\n"));
        let parsed: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut pem.as_bytes()).collect::<Result<_, _>>()?;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_ref(), cert.cert_der.as_slice());
        Ok(())
    }
}
