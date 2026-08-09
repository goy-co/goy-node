//! Conexão WebSocket com o relay local (strfry) + health check NIP-11.

use std::sync::Arc;
use std::time::Duration;

use backoff::ExponentialBackoffBuilder;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::RelayConfig;

/// Evento recebido do relay local, pronto para ser encaminhado ao mesh.
#[derive(Debug, Clone)]
pub struct RelayEvent {
    pub raw: String,
}

/// Conecta ao relay local, mantém subscription viva e publica eventos
/// recebidos no canal de saída. Reconnecta automaticamente em caso de falha.
pub async fn connect(
    cfg: RelayConfig,
    cancel: CancellationToken,
) -> anyhow::Result<broadcast::Receiver<RelayEvent>> {
    let (tx, rx) = broadcast::channel::<RelayEvent>(1024);

    // Verifica se o relay está vivo antes de iniciar o loop
    if let Err(e) = health_check(&cfg.url).await {
        warn!("⚠️  Relay não disponível em {}: {e}. Aguardando…", cfg.url);
    } else {
        info!("✔ Relay local disponível em {}", cfg.url);
    }

    let url = cfg.url.clone();
    tokio::spawn(async move {
        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_secs(1))
            .with_max_interval(Duration::from_secs(60))
            .with_max_elapsed_time(None) // retry forever
            .build();

        backoff::future::retry(backoff, || async {
            if cancel.is_cancelled() {
                return Err::<(), _>(backoff::Error::permanent(anyhow::anyhow!("shutdown")));
            }

            match run_session(&url, &tx).await {
                Ok(()) => Err::<(), _>(backoff::Error::permanent(anyhow::anyhow!("session ended"))),
                Err(e) => {
                    warn!("🔌 Relay connection lost: {e}. Reconnecting…");
                    Err::<(), _>(backoff::Error::transient(e))
                }
            }
        })
        .await
        .ok();

        info!("🔌 Relay connection loop stopped");
    });

    Ok(rx)
}

/// Health check via NIP-11 (GET com Accept: application/nostr+json).
async fn health_check(ws_url: &str) -> anyhow::Result<()> {
    let http_url = ws_url
        .replace("ws://", "http://")
        .replace("wss://", "https://");

    let resp = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?
        .get(&http_url)
        .header("Accept", "application/nostr+json")
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("NIP-11 returned status {}", resp.status());
    }

    Ok(())
}

/// Uma sessão WebSocket completa com o relay.
/// Retorna Ok(()) apenas em shutdown gracioso; Err em qualquer falha.
async fn run_session(url: &str, tx: &broadcast::Sender<RelayEvent>) -> anyhow::Result<()> {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (mut ws, _) = tokio_tungstenite::connect_async(url).await?;
    info!("🟢 Connected to relay at {url}");

    // Envia REQ para receber eventos novos (live sync)
    let sub_msg = r#"["REQ","goy-live",{"since":0}]"#;
    ws.send(Message::Text(sub_msg.into())).await?;
    info!("📡 Subscribed to live events");

    while let Some(msg) = ws.next().await {
        let msg = msg?;
        match msg {
            Message::Text(text) => {
                // Encaminha eventos EVENT para o mesh agent
                // Formato: ["EVENT", subscription_id, event_json]
                if text.starts_with(r#"["EVENT""#) {
                    let _ = tx.send(RelayEvent {
                        raw: text.to_string(),
                    });
                }
                // EOSE, OK, NOTICE são logados mas não encaminhados
                else if text.starts_with(r#"["EOSE""#) {
                    info!("📨 Received EOSE from relay");
                }
                // Outros mensajes podem ser tratados aqui
            }
            Message::Ping(data) => {
                ws.send(Message::Pong(data)).await?;
            }
            Message::Close(_) => {
                warn!("🔌 Relay closed connection");
                anyhow::bail!("relay closed connection");
            }
            _ => {} // Binary, Frame, etc. — ignorar
        }
    }

    anyhow::bail!("websocket stream ended unexpectedly")
}

/// Cria um canal para publicar eventos no relay local via WebSocket.
/// Retorna o sender (para o mesh agent) e spawn a task de escrita.
/// Cria um canal para publicar eventos no relay local via WebSocket.
/// Retorna o sender (para o mesh agent) e spawn a task de escrita.
pub fn create_publisher(cfg: &RelayConfig, cancel: CancellationToken) -> mpsc::Sender<String> {
    let (tx, rx) = mpsc::channel::<String>(256);
    let url = cfg.url.clone();

    // Wrap rx em Arc<Mutex> para permitir acesso através de FnMut closures
    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    tokio::spawn(async move {
        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_secs(1))
            .with_max_interval(Duration::from_secs(60))
            .with_max_elapsed_time(None)
            .build();

        backoff::future::retry(backoff, || {
            let rx = rx.clone();
            let url = url.clone();
            let cancel = cancel.clone();
            async move {
                if cancel.is_cancelled() {
                    return Err::<(), _>(backoff::Error::permanent(anyhow::anyhow!("shutdown")));
                }

                match publisher_session(&url, rx).await {
                    Ok(()) => {
                        Err::<(), _>(backoff::Error::permanent(anyhow::anyhow!("session ended")))
                    }
                    Err(e) => {
                        warn!("🔌 Publisher connection lost: {e}. Reconnecting…");
                        Err::<(), _>(backoff::Error::transient(e))
                    }
                }
            }
        })
        .await
        .ok();
    });

    tx
}

async fn publisher_session(
    url: &str,
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<String>>>,
) -> anyhow::Result<()> {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (mut ws, _) = tokio_tungstenite::connect_async(url).await?;
    info!("📤 Publisher connected to relay at {url}");

    loop {
        // Lock o mutex apenas para receber a próxima mensagem
        let raw = {
            let mut rx_guard = rx.lock().await;
            rx_guard.recv().await
        };

        match raw {
            Some(event_json) => {
                // Se já veio formatado como ["EVENT",...], envia direto; senão, formata
                let to_send = if event_json.starts_with(r#"["EVENT""#) {
                    event_json
                } else {
                    format!(r#"["EVENT",{}]"#, event_json)
                };

                if ws.send(Message::Text(to_send.into())).await.is_err() {
                    anyhow::bail!("publisher websocket send failed");
                }

                // Aguarda OK do relay (não-bloqueante com timeout)
                tokio::time::timeout(Duration::from_secs(5), async {
                    while let Some(Ok(Message::Text(resp))) = ws.next().await {
                        if resp.starts_with(r#"["OK""#) {
                            break;
                        }
                    }
                })
                .await
                .ok();
            }
            None => {
                // Canal fechado = shutdown gracioso
                anyhow::bail!("publisher channel closed");
            }
        }
    }
}
