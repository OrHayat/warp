use std::path::{Path, PathBuf};

use watcher::BulkFilesystemWatcherEvent;

use super::{affected_keys, kubeconfig_files, watch_paths};

#[test]
fn test_kubeconfig_files_defaults_to_home_kube_config() {
    let home = Path::new("/home/user");
    let expected = vec![PathBuf::from("/home/user/.kube/config")];
    assert_eq!(kubeconfig_files(None, home), Some(expected.clone()));
    assert_eq!(kubeconfig_files(Some(""), home), Some(expected));
}

#[cfg(unix)]
#[test]
fn test_kubeconfig_files_keeps_kubeconfig_order() {
    assert_eq!(
        kubeconfig_files(Some("/b/config:/a/config::"), Path::new("/home/user")),
        Some(vec![PathBuf::from("/b/config"), PathBuf::from("/a/config")])
    );
}

#[cfg(unix)]
#[test]
fn test_kubeconfig_files_rejects_relative_entries() {
    assert_eq!(
        kubeconfig_files(Some("/a/config:dev/config"), Path::new("/home/user")),
        None
    );
}

#[test]
fn test_affected_keys_matches_written_and_renamed_files() {
    let default_key = vec![PathBuf::from("/home/user/.kube/config")];
    let other_key = vec![PathBuf::from("/srv/kube/prod.yaml")];
    let keys = [
        (&default_key, default_key.as_slice()),
        (&other_key, other_key.as_slice()),
    ];

    let modified = BulkFilesystemWatcherEvent {
        modified: [PathBuf::from("/home/user/.kube/config")].into(),
        ..Default::default()
    };
    assert_eq!(affected_keys(keys, &modified), vec![default_key.clone()]);

    // kubectl writes a temp file and renames it over the kubeconfig.
    let renamed = BulkFilesystemWatcherEvent {
        moved: [(
            PathBuf::from("/srv/kube/prod.yaml"),
            PathBuf::from("/srv/kube/prod.yaml.lock"),
        )]
        .into(),
        ..Default::default()
    };
    assert_eq!(affected_keys(keys, &renamed), vec![other_key.clone()]);

    let unrelated = BulkFilesystemWatcherEvent {
        added: [PathBuf::from("/home/user/.kube/cache")].into(),
        ..Default::default()
    };
    assert!(affected_keys(keys, &unrelated).is_empty());
}

#[cfg(unix)]
#[test]
fn test_symlinked_kubeconfig_is_affected_by_edits_to_its_target() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("dotfiles-kubeconfig");
    std::fs::write(&target, "").unwrap();
    let link = dir.path().join("config");
    std::os::unix::fs::symlink(&target, &link).unwrap();

    let key = vec![link.clone()];
    let paths = watch_paths(&key);
    assert_eq!(paths, vec![link, target.canonicalize().unwrap()]);

    let edited_target = BulkFilesystemWatcherEvent {
        modified: [target.canonicalize().unwrap()].into(),
        ..Default::default()
    };
    assert_eq!(
        affected_keys([(&key, paths.as_slice())], &edited_target),
        vec![key.clone()]
    );
}
