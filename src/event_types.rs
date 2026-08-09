//! Módulo para classificação e chaveamento de eventos Nostr (NIP-01, NIP-09, NIP-16, NIP-33).

use serde_json::Value;

/// Verifica se o `kind` é de um evento substituível (replaceable ou parameterized replaceable).
/// NIP-01 / NIP-16 / NIP-33:
/// - Kind 0 (metadata), Kind 3 (contacts), Kind 10002 (relay list)
/// - Range 10000..=19999 (replaceable events)
/// - Range 30000..=39999 (parameterized replaceable events)
pub fn is_replaceable(kind: u64) -> bool {
    kind == 0
        || kind == 3
        || kind == 10002
        || (10000..=19999).contains(&kind)
        || is_parameterized_replaceable(kind)
}

/// Verifica se o `kind` é de um evento substituível parametrizado (parameterized replaceable).
/// NIP-33: Range 30000..=39999.
pub fn is_parameterized_replaceable(kind: u64) -> bool {
    (30000..=39999).contains(&kind)
}

/// Extrai o valor da tag "d" de um evento Nostr JSON (para parameterized replaceable).
pub fn extract_d_tag(event: &Value) -> String {
    if let Some(tags) = event.get("tags").and_then(|t| t.as_array()) {
        for tag in tags {
            if let Some(tag_arr) = tag.as_array() {
                if tag_arr.len() >= 2 && tag_arr[0].as_str() == Some("d") {
                    return tag_arr[1].as_str().unwrap_or("").to_string();
                }
            }
        }
    }
    String::new()
}

/// Calcula a chave única de substituição de um evento Nostr JSON.
/// - Replaceable normal: "{pubkey}:{kind}"
/// - Parameterized replaceable: "{pubkey}:{kind}:{d_tag}"
/// Retorna `None` se o evento não for substituível ou se faltarem campos obrigatórios (`pubkey`/`kind`).
pub fn replacement_key(event: &Value) -> Option<String> {
    let kind = event.get("kind")?.as_u64()?;
    if !is_replaceable(kind) {
        return None;
    }
    let pubkey = event.get("pubkey")?.as_str()?;

    if is_parameterized_replaceable(kind) {
        let d_tag = extract_d_tag(event);
        Some(format!("{pubkey}:{kind}:{d_tag}"))
    } else {
        Some(format!("{pubkey}:{kind}"))
    }
}

/// Verifica se o `kind` é um evento de deleção (NIP-09).
/// Kind 5 = evento de deleção.
pub fn is_deletion(kind: u64) -> bool {
    kind == 5
}

/// Extrai os event IDs referenciados nas tags `e` de um evento de deleção (NIP-09).
/// Retorna Vec de IDs a deletar.
pub fn extract_e_tags(event: &Value) -> Vec<String> {
    let mut ids = Vec::new();
    if let Some(tags) = event.get("tags").and_then(|t| t.as_array()) {
        for tag in tags {
            if let Some(tag_arr) = tag.as_array() {
                if tag_arr.len() >= 2 && tag_arr[0].as_str() == Some("e") {
                    if let Some(id) = tag_arr[1].as_str() {
                        ids.push(id.to_string());
                    }
                }
            }
        }
    }
    ids
}

/// Extrai as coordenadas de eventos substituíveis nas tags `a` de um evento de deleção (NIP-09).
/// Formato da coordenada: "kind:pubkey" ou "kind:pubkey:d_tag".
/// Retorna Vec de chaves de substituição (replacement keys).
pub fn extract_a_tags(event: &Value) -> Vec<String> {
    let mut keys = Vec::new();
    if let Some(tags) = event.get("tags").and_then(|t| t.as_array()) {
        for tag in tags {
            if let Some(tag_arr) = tag.as_array() {
                if tag_arr.len() >= 2 && tag_arr[0].as_str() == Some("a") {
                    if let Some(coord) = tag_arr[1].as_str() {
                        // Coordenada formato NIP-33: "kind:pubkey" ou "kind:pubkey:d_tag"
                        // Converter para o formato de replacement_key: "pubkey:kind" ou "pubkey:kind:d_tag"
                        let parts: Vec<&str> = coord.splitn(3, ':').collect();
                        if parts.len() >= 2 {
                            let kind_str = parts[0];
                            let pubkey = parts[1];
                            let d_tag = parts.get(2).copied().unwrap_or("");
                            if d_tag.is_empty() {
                                keys.push(format!("{pubkey}:{kind_str}"));
                            } else {
                                keys.push(format!("{pubkey}:{kind_str}:{d_tag}"));
                            }
                        }
                    }
                }
            }
        }
    }
    keys
}

/// Extrai o objeto de evento a partir de uma mensagem Nostr JSON raw.
/// Suporta formatos:
/// - ["EVENT", {"id":"...", "pubkey":"...", "kind":...}]
/// - ["EVENT", "sub_id", {"id":"...", "pubkey":"...", "kind":...}]
pub fn extract_event_object(raw: &str) -> Option<Value> {
    let parsed: Value = serde_json::from_str(raw).ok()?;
    let arr = parsed.as_array()?;
    if arr.is_empty() || arr[0].as_str() != Some("EVENT") {
        return None;
    }
    if arr.len() == 2 {
        Some(arr[1].clone())
    } else if arr.len() >= 3 {
        Some(arr[2].clone())
    } else {
        None
    }
}

/// Extrai o valor da tag `expiration` de um evento Nostr (NIP-40).
/// Retorna `Some(timestamp_unix)` se a tag existir e o valor for um inteiro válido,
/// ou `None` se a tag não existir ou o valor não for parsável.
pub fn extract_expiration(event: &Value) -> Option<u64> {
    let tags = event.get("tags")?.as_array()?;
    for tag in tags {
        let tag_arr = tag.as_array()?;
        if tag_arr.len() >= 2 && tag_arr[0].as_str() == Some("expiration") {
            let raw = tag_arr[1].as_str()?;
            return raw.parse::<u64>().ok();
        }
    }
    None
}

/// Verifica se um evento está expirado em relação ao instante `now_ts` (timestamp Unix).
/// Retorna `true` se o evento tem tag `expiration` e o valor é <= `now_ts`.
pub fn is_expired(event: &Value, now_ts: u64) -> bool {
    match extract_expiration(event) {
        Some(exp) => exp <= now_ts,
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_is_replaceable() {
        assert!(is_replaceable(0));
        assert!(is_replaceable(3));
        assert!(is_replaceable(10002));
        assert!(is_replaceable(10000));
        assert!(is_replaceable(15000));
        assert!(is_replaceable(19999));
        assert!(is_replaceable(30000));
        assert!(is_replaceable(30001));
        assert!(is_replaceable(39999));

        assert!(!is_replaceable(1));
        assert!(!is_replaceable(7));
        assert!(!is_replaceable(9999));
        assert!(!is_replaceable(20000));
        assert!(!is_replaceable(40000));
    }

    #[test]
    fn test_is_parameterized_replaceable() {
        assert!(is_parameterized_replaceable(30000));
        assert!(is_parameterized_replaceable(30001));
        assert!(is_parameterized_replaceable(39999));

        assert!(!is_parameterized_replaceable(0));
        assert!(!is_parameterized_replaceable(3));
        assert!(!is_parameterized_replaceable(10000));
    }

    #[test]
    fn test_extract_d_tag() {
        let event_with_d = json!({
            "kind": 30001,
            "pubkey": "pub123",
            "tags": [
                ["t", "nostr"],
                ["d", "my-identifier"],
                ["p", "otherpub"]
            ]
        });
        assert_eq!(extract_d_tag(&event_with_d), "my-identifier");

        let event_without_d = json!({
            "kind": 30001,
            "pubkey": "pub123",
            "tags": [["t", "nostr"]]
        });
        assert_eq!(extract_d_tag(&event_without_d), "");
    }

    #[test]
    fn test_is_deletion() {
        assert!(is_deletion(5));
        assert!(!is_deletion(0));
        assert!(!is_deletion(1));
        assert!(!is_deletion(3));
    }

    #[test]
    fn test_extract_e_tags() {
        let event = json!({
            "kind": 5,
            "pubkey": "pub_alice",
            "tags": [
                ["e", "event_id_1"],
                ["e", "event_id_2"],
                ["p", "other_pubkey"]
            ]
        });
        let ids = extract_e_tags(&event);
        assert_eq!(ids, vec!["event_id_1", "event_id_2"]);
    }

    #[test]
    fn test_extract_e_tags_empty() {
        let event = json!({"kind": 5, "tags": [["p", "other"]]});
        assert!(extract_e_tags(&event).is_empty());
    }

    #[test]
    fn test_extract_a_tags() {
        let event = json!({
            "kind": 5,
            "pubkey": "pub_alice",
            "tags": [
                ["a", "0:pub_alice"],
                ["a", "30001:pub_bob:my-list"],
                ["e", "event_id_1"]
            ]
        });
        let keys = extract_a_tags(&event);
        // Conversion: "kind:pubkey" -> "pubkey:kind", "kind:pubkey:d" -> "pubkey:kind:d"
        assert!(keys.contains(&"pub_alice:0".to_string()));
        assert!(keys.contains(&"pub_bob:30001:my-list".to_string()));
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_replacement_key() {
        let normal_rep = json!({
            "kind": 0,
            "pubkey": "pub_alice",
            "created_at": 1000
        });
        assert_eq!(replacement_key(&normal_rep), Some("pub_alice:0".to_string()));

        let param_rep = json!({
            "kind": 30001,
            "pubkey": "pub_bob",
            "tags": [["d", "list-1"]]
        });
        assert_eq!(replacement_key(&param_rep), Some("pub_bob:30001:list-1".to_string()));

        let text_note = json!({
            "kind": 1,
            "pubkey": "pub_carol",
            "content": "hello"
        });
        assert_eq!(replacement_key(&text_note), None);
    }

    #[test]
    fn test_extract_expiration() {
        let event_with_exp = json!({
            "kind": 1,
            "tags": [["t", "nostr"], ["expiration", "9999999999"]]
        });
        assert_eq!(extract_expiration(&event_with_exp), Some(9999999999u64));

        let event_without_exp = json!({"kind": 1, "tags": [["t", "nostr"]]});
        assert_eq!(extract_expiration(&event_without_exp), None);

        let event_invalid_exp = json!({"kind": 1, "tags": [["expiration", "not-a-number"]]});
        assert_eq!(extract_expiration(&event_invalid_exp), None);

        let event_no_tags = json!({"kind": 1});
        assert_eq!(extract_expiration(&event_no_tags), None);
    }

    #[test]
    fn test_is_expired() {
        let now = 1_700_000_000u64;

        let expired_event = json!({"kind": 1, "tags": [["expiration", "1699999999"]]});
        assert!(is_expired(&expired_event, now), "Event with past expiration must be expired");

        let at_boundary = json!({"kind": 1, "tags": [["expiration", "1700000000"]]});
        assert!(is_expired(&at_boundary, now), "Event with expiration == now must be considered expired");

        let future_event = json!({"kind": 1, "tags": [["expiration", "1700000001"]]});
        assert!(!is_expired(&future_event, now), "Event with future expiration must NOT be expired");

        let no_expiry = json!({"kind": 1, "tags": []});
        assert!(!is_expired(&no_expiry, now), "Event without expiration tag must never be expired");
    }
}
