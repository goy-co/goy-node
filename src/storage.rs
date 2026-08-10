// Escolha de crate/implementação de estatísticas do sistema de ficheiros:
// Optou-se pela utilização direta da crate `libc` com a chamada POSIX `statvfs` para plataformas Unix
// (Linux e macOS). Esta solução é nativa, extremamente rápida, leve (zero dependências adicionais
// por já estar na árvore do projecto) e cumpre todos os requisitos de inspeção de partições de disco.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Mínimo de storage reservado em gigabytes por nó no Goy Network (50 GB).
///
/// Esta constante é hardcoded no código-fonte e não-configurável para garantir um contrato social de
/// contribuição mínima à redundância da rede, capacidade para ~5-10M eventos Nostr típicos
/// e suporte garantido em hardware moderno.
pub const MIN_RESERVED_GB: u64 = 50;

/// Código de saída do processo (exit code 3) quando ocorre falha crítica na verificação de armazenamento.
pub const EXIT_STORAGE_ERROR: i32 = 3;

fn default_data_dir() -> PathBuf {
    PathBuf::from("/var/lib/goy-node")
}

/// Struct dedicada para configuração de armazenamento reservado do nó.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageConfig {
    /// Espaço de armazenamento adicional voluntário em GB (além do mínimo obrigatório de 50 GB).
    #[serde(default)]
    pub extra_contribution_gb: u64,
    /// Caminho para o diretório de dados do nó (chaves, certificados, estado persistente).
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            extra_contribution_gb: 0,
            data_dir: default_data_dir(),
        }
    }
}

/// Resultado de uma verificação de armazenamento bem-sucedida.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageInfo {
    /// Soma de MIN_RESERVED_GB + extra_contribution_gb em GB.
    pub total_reserved_gb: u64,
    /// Espaço livre atual no filesystem do data_dir em GB.
    pub available_gb: u64,
    /// Espaço já ocupado pelo goy-node no data_dir (tamanho recursivo) em GB.
    pub used_gb: u64,
    /// Caminho do filesystem verificado (resolvido se for symlink ou mount point).
    pub filesystem_path: PathBuf,
}

/// Estrutura para exportação de métricas leves de armazenamento (em bytes).
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageMetrics {
    /// Bytes totais reservados pelo nó (MIN_RESERVED_GB + extra em bytes).
    pub reserved_bytes: u64,
    /// Bytes disponíveis no filesystem do data_dir.
    pub available_bytes: u64,
    /// Bytes utilizados no data_dir pelo goy-node.
    pub used_bytes: u64,
}

/// Erros decorrentes de verificações de armazenamento do nó.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// O disco tem menos espaço disponível do que o mínimo obrigatório de 50 GB.
    InsufficientSpace { available_gb: u64, required_gb: u64 },
    /// Diretório de dados não existe e não pôde ser criado.
    DataDirNotFound(PathBuf),
    /// Sem permissões de leitura/escrita no data_dir.
    PermissionDenied(PathBuf),
    /// Erro genérico do sistema de ficheiros.
    FilesystemError(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::InsufficientSpace {
                available_gb,
                required_gb,
            } => {
                write!(
                    f,
                    "Espaço em disco insuficiente: {available_gb} GB disponíveis, {required_gb} GB requeridos (mínimo obrigatório). Liberta espaço ou escolhe outro data_dir."
                )
            }
            StorageError::DataDirNotFound(path) => {
                write!(
                    f,
                    "Diretório de dados não encontrado e não foi possível criá-lo em '{}'.",
                    path.display()
                )
            }
            StorageError::PermissionDenied(path) => {
                write!(
                    f,
                    "Permissão negada ao aceder ou escrever no diretório de dados '{}'.",
                    path.display()
                )
            }
            StorageError::FilesystemError(msg) => {
                write!(f, "Erro no sistema de ficheiros: {msg}")
            }
        }
    }
}

impl std::error::Error for StorageError {}

/// Função principal de verificação de espaço em disco.
///
/// Garante que o `data_dir` existe (criando-o se necessário), verifica permissões de escrita,
/// calcula o espaço utilizado recursivamente e compara o espaço disponível no filesystem
/// contra o mínimo obrigatório (`MIN_RESERVED_GB` = 50 GB).
#[allow(dead_code)]
pub fn verify_storage(config: &StorageConfig) -> Result<StorageInfo, StorageError> {
    let data_dir = &config.data_dir;

    // 1. Verificar existência do data_dir. Se não existir, tentar criar.
    if !data_dir.exists() {
        if let Err(err) = std::fs::create_dir_all(data_dir) {
            if err.kind() == std::io::ErrorKind::PermissionDenied {
                return Err(StorageError::PermissionDenied(data_dir.clone()));
            }
            return Err(StorageError::DataDirNotFound(data_dir.clone()));
        }
        info!("📁 Diretório de dados criado em '{}'", data_dir.display());
    }

    // Resolver symlinks e obter o caminho real do diretório
    let resolved_path = match std::fs::canonicalize(data_dir) {
        Ok(p) => p,
        Err(_) => data_dir.clone(),
    };

    // Testar permissões parciais/escrita no data_dir criando um ficheiro temporário
    let test_file = resolved_path.join(".goy_storage_perm_check");
    match std::fs::write(&test_file, b"test") {
        Ok(_) => {
            let _ = std::fs::remove_file(&test_file);
        }
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            return Err(StorageError::PermissionDenied(resolved_path));
        }
        Err(err) => {
            return Err(StorageError::FilesystemError(format!(
                "Sem permissão de escrita no diretório de dados '{}': {err}",
                resolved_path.display()
            )));
        }
    }

    // 2. Obter estatísticas do filesystem onde o data_dir reside
    let (_total_bytes, available_bytes) = get_filesystem_stats(&resolved_path)?;
    let available_gb = available_bytes / (1024 * 1024 * 1024);

    // 3. Calcular espaço usado pelo goy-node no data_dir (tamanho recursivo)
    let used_bytes = calculate_directory_size_bytes(&resolved_path);
    let used_gb = used_bytes / (1024 * 1024 * 1024);

    // 4. Calcular total reservado (MIN_RESERVED_GB + extra)
    let total_reserved_gb = MIN_RESERVED_GB.saturating_add(config.extra_contribution_gb);

    // 5. Comparar disponível vs mínimo obrigatório (MIN_RESERVED_GB)
    if available_gb < MIN_RESERVED_GB {
        return Err(StorageError::InsufficientSpace {
            available_gb,
            required_gb: MIN_RESERVED_GB,
        });
    }

    // 6. Retornar StorageInfo
    Ok(StorageInfo {
        total_reserved_gb,
        available_gb,
        used_gb,
        filesystem_path: resolved_path,
    })
}

/// Função de métricas leves de armazenamento (em bytes).
///
/// Retorna valores numéricos precisos em bytes para o exportador Prometheus sem validações pesadas.
#[allow(dead_code)]
pub fn get_storage_metrics(config: &StorageConfig) -> Result<StorageMetrics, StorageError> {
    let data_dir = &config.data_dir;
    let target_path = if data_dir.exists() {
        data_dir.as_path()
    } else {
        data_dir.parent().unwrap_or(data_dir)
    };

    let (_total_bytes, available_bytes) = get_filesystem_stats(target_path)?;
    let used_bytes = calculate_directory_size_bytes(data_dir);

    let total_reserved_gb = MIN_RESERVED_GB.saturating_add(config.extra_contribution_gb);
    let reserved_bytes = total_reserved_gb.saturating_mul(1_073_741_824);

    Ok(StorageMetrics {
        reserved_bytes,
        available_bytes,
        used_bytes,
    })
}

/// Obtém estatísticas de espaço total e disponível do filesystem onde o caminho reside.
#[allow(dead_code)]
#[cfg(unix)]
pub fn get_filesystem_stats(path: &Path) -> Result<(u64, u64), StorageError> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| StorageError::FilesystemError(format!("Caminho inválido para FFI: {e}")))?;

    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let res = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };

    if res != 0 {
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::PermissionDenied {
            return Err(StorageError::PermissionDenied(path.to_path_buf()));
        }
        return Err(StorageError::FilesystemError(format!(
            "Erro ao obter estatísticas do filesystem para '{}': {err}",
            path.display()
        )));
    }

    let stat = unsafe { stat.assume_init() };

    #[allow(clippy::unnecessary_cast)]
    let block_size = if stat.f_frsize > 0 {
        stat.f_frsize as u64
    } else {
        stat.f_bsize as u64
    };

    #[allow(clippy::unnecessary_cast)]
    let total_bytes = (stat.f_blocks as u64).saturating_mul(block_size);
    #[allow(clippy::unnecessary_cast)]
    let available_bytes = (stat.f_bavail as u64).saturating_mul(block_size);

    check_remote_filesystem(path);

    Ok((total_bytes, available_bytes))
}

#[allow(dead_code)]
#[cfg(not(unix))]
pub fn get_filesystem_stats(_path: &Path) -> Result<(u64, u64), StorageError> {
    Ok((1_000_000_000_000, 500_000_000_000))
}

#[allow(dead_code)]
fn check_remote_filesystem(path: &Path) {
    let path_str = path.to_string_lossy().to_ascii_lowercase();
    if path_str.contains("nfs") || path_str.contains("smb") || path_str.contains("cifs") {
        info!(
            "ℹ️  Diretório de dados '{}' está localizado num sistema de ficheiros remoto (NFS/SMB). A performance e fiabilidade podem variar.",
            path.display()
        );
    }
}

/// Calcula recursivamente o tamanho total em bytes dos ficheiros contidos num diretório.
///
/// É tolerante a erros de permissão em subdiretórios individuais (emite warning e prossegue).
#[allow(dead_code)]
pub fn calculate_directory_size_bytes(dir: &Path) -> u64 {
    if !dir.exists() || !dir.is_dir() {
        return 0;
    }

    let mut total_size: u64 = 0;
    let mut stack = vec![dir.to_path_buf()];

    while let Some(current_dir) = stack.pop() {
        let entries = match std::fs::read_dir(&current_dir) {
            Ok(e) => e,
            Err(err) => {
                warn!(
                    "⚠️  Não foi possível ler diretório '{}' durante cálculo de tamanho: {err}",
                    current_dir.display()
                );
                continue;
            }
        };

        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(err) => {
                    warn!("⚠️  Erro ao aceder entrada de diretório: {err}");
                    continue;
                }
            };

            let path = entry.path();
            let metadata = match entry.metadata() {
                Ok(m) => m,
                Err(err) => {
                    warn!("⚠️  Erro ao obter metadata de '{}': {err}", path.display());
                    continue;
                }
            };

            if metadata.is_file() {
                total_size = total_size.saturating_add(metadata.len());
            } else if metadata.is_dir() && !metadata.is_symlink() {
                stack.push(path);
            }
        }
    }

    total_size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_min_reserved_gb_constant() {
        assert_eq!(MIN_RESERVED_GB, 50);
    }

    #[test]
    fn test_storage_config_default() {
        let cfg = StorageConfig::default();
        assert_eq!(cfg.extra_contribution_gb, 0);
        assert_eq!(cfg.data_dir, PathBuf::from("/var/lib/goy-node"));
    }

    #[test]
    fn test_total_reserved_gb_calculation() {
        let cfg1 = StorageConfig {
            extra_contribution_gb: 0,
            data_dir: PathBuf::from("/tmp"),
        };
        assert_eq!(MIN_RESERVED_GB + cfg1.extra_contribution_gb, 50);

        let cfg2 = StorageConfig {
            extra_contribution_gb: 150,
            data_dir: PathBuf::from("/tmp"),
        };
        assert_eq!(MIN_RESERVED_GB + cfg2.extra_contribution_gb, 200);
    }

    #[test]
    fn test_calculate_directory_size_bytes() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let file1 = temp_dir.path().join("file1.txt");
        let file2 = temp_dir.path().join("sub/file2.txt");

        std::fs::create_dir_all(temp_dir.path().join("sub"))?;
        std::fs::write(&file1, vec![0u8; 1000])?;
        std::fs::write(&file2, vec![0u8; 2500])?;

        let size = calculate_directory_size_bytes(temp_dir.path());
        assert_eq!(size, 3500);
        Ok(())
    }

    #[test]
    fn test_verify_storage_creates_nonexistent_dir() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let new_data_dir = temp_dir.path().join("new_nested/data");

        assert!(!new_data_dir.exists());

        let cfg = StorageConfig {
            extra_contribution_gb: 0,
            data_dir: new_data_dir.clone(),
        };

        let result = verify_storage(&cfg);

        assert!(new_data_dir.exists());
        match result {
            Ok(info) => {
                assert_eq!(info.total_reserved_gb, 50);
                assert_eq!(info.used_gb, 0);
            }
            Err(StorageError::InsufficientSpace {
                available_gb,
                required_gb,
            }) => {
                assert!(available_gb < required_gb);
            }
            Err(e) => panic!("Unexpected error: {e}"),
        }

        Ok(())
    }

    #[test]
    fn test_get_storage_metrics_consistency() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let cfg = StorageConfig {
            extra_contribution_gb: 50,
            data_dir: temp_dir.path().to_path_buf(),
        };

        let metrics = get_storage_metrics(&cfg)?;

        // 50 MIN + 50 EXTRA = 100 GB = 100 * 1073741824 bytes
        assert_eq!(metrics.reserved_bytes, 100 * 1_073_741_824);
        assert_eq!(metrics.used_bytes, 0);
        assert!(metrics.available_bytes > 0);

        Ok(())
    }

    #[test]
    fn test_storage_error_display() {
        let err1 = StorageError::InsufficientSpace {
            available_gb: 20,
            required_gb: 50,
        };
        assert!(
            err1.to_string()
                .contains("20 GB disponíveis, 50 GB requeridos")
        );

        let err2 = StorageError::DataDirNotFound(PathBuf::from("/invalid/dir"));
        assert!(err2.to_string().contains("'/invalid/dir'"));

        let err3 = StorageError::PermissionDenied(PathBuf::from("/root/secret"));
        assert!(err3.to_string().contains("Permissão negada"));

        let err4 = StorageError::FilesystemError("IO Error".to_string());
        assert!(err4.to_string().contains("IO Error"));
    }

    #[test]
    fn test_nonexistent_dir_creation_failure() {
        // Tentar criar num diretório inválido/impossível (ex: subdiretório de um ficheiro)
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let invalid_dir = temp_file.path().join("impossible_sub_dir");

        let cfg = StorageConfig {
            extra_contribution_gb: 0,
            data_dir: invalid_dir,
        };

        let res = verify_storage(&cfg);
        assert!(res.is_err());
    }

    #[test]
    fn test_calculate_directory_size_bytes_handles_symlinks() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let target_file = temp_dir.path().join("target.txt");
        std::fs::write(&target_file, vec![0u8; 1000])?;

        let symlink_path = temp_dir.path().join("symlink.txt");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target_file, &symlink_path)?;

        let size = calculate_directory_size_bytes(temp_dir.path());
        assert_eq!(size, 1000);
        Ok(())
    }

    #[test]
    fn test_concurrent_get_storage_metrics() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let cfg = std::sync::Arc::new(StorageConfig {
            extra_contribution_gb: 10,
            data_dir: temp_dir.path().to_path_buf(),
        });

        let mut handles = vec![];
        for _ in 0..10 {
            let cfg_clone = cfg.clone();
            handles.push(std::thread::spawn(move || get_storage_metrics(&cfg_clone)));
        }

        for handle in handles {
            let res = handle.join().unwrap();
            assert!(res.is_ok());
            let metrics = res.unwrap();
            assert_eq!(metrics.reserved_bytes, 60 * 1_073_741_824);
        }

        Ok(())
    }
}
