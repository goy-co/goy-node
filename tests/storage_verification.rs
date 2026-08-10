use std::path::PathBuf;

use goy_node::storage::{
    self, MIN_RESERVED_GB, StorageConfig, StorageError, StorageInfo, verify_storage,
};

#[test]
fn test_startup_storage_verification_success_flow() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let config = StorageConfig {
        extra_contribution_gb: 25,
        data_dir: temp_dir.path().to_path_buf(),
    };

    let result = verify_storage(&config);

    match result {
        Ok(info) => {
            assert_eq!(info.total_reserved_gb, 75);
            assert_eq!(info.used_gb, 0);
            assert!(info.available_gb > 0);
            assert_eq!(
                info.filesystem_path,
                std::fs::canonicalize(temp_dir.path())?
            );
        }
        Err(StorageError::InsufficientSpace {
            available_gb,
            required_gb,
        }) => {
            // Em ambientes de CI com disco pequeno
            assert_eq!(required_gb, MIN_RESERVED_GB);
            assert!(available_gb < MIN_RESERVED_GB);
        }
        Err(e) => panic!("Erro inesperado na verificação de storage: {e}"),
    }

    Ok(())
}

#[test]
fn test_startup_storage_verification_permission_denied() {
    // Tenta usar um caminho em que não é possível criar/escrever ficheiros
    let invalid_path = PathBuf::from("/proc/impossible_goy_dir/data");
    let config = StorageConfig {
        extra_contribution_gb: 0,
        data_dir: invalid_path.clone(),
    };

    let result = verify_storage(&config);
    assert!(result.is_err());

    let err = result.unwrap_err();
    match err {
        StorageError::PermissionDenied(p) => assert_eq!(p, invalid_path),
        StorageError::DataDirNotFound(p) => assert_eq!(p, invalid_path),
        StorageError::FilesystemError(_) => {}
        StorageError::InsufficientSpace { .. } => panic!("Não deve ser InsufficientSpace"),
    }
}

#[test]
fn test_storage_exit_code_constant() {
    assert_eq!(storage::EXIT_STORAGE_ERROR, 3);
}

#[test]
fn test_storage_info_struct_fields() -> anyhow::Result<()> {
    let info = StorageInfo {
        total_reserved_gb: 150,
        available_gb: 300,
        used_gb: 5,
        filesystem_path: PathBuf::from("/var/lib/goy-node"),
    };

    assert_eq!(info.total_reserved_gb, 150);
    assert_eq!(info.available_gb, 300);
    assert_eq!(info.used_gb, 5);
    assert_eq!(info.filesystem_path, PathBuf::from("/var/lib/goy-node"));
    Ok(())
}
