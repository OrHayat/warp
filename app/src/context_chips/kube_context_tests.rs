use std::path::{Path, PathBuf};

use warpui::{App, SingletonEntity};
use watcher::BulkFilesystemWatcherEvent;

use super::{KubeContextModel, kubeconfig_files, touches, watch_paths};

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
fn test_touches_matches_written_and_renamed_files() {
    let paths = [PathBuf::from("/home/user/.kube/config")];

    let modified = BulkFilesystemWatcherEvent {
        modified: [PathBuf::from("/home/user/.kube/config")].into(),
        ..Default::default()
    };
    assert!(touches(&paths, &modified));

    // kubectl writes a temp file and renames it over the kubeconfig.
    let renamed = BulkFilesystemWatcherEvent {
        moved: [(
            PathBuf::from("/home/user/.kube/config"),
            PathBuf::from("/home/user/.kube/config.lock"),
        )]
        .into(),
        ..Default::default()
    };
    assert!(touches(&paths, &renamed));

    let unrelated = BulkFilesystemWatcherEvent {
        added: [PathBuf::from("/home/user/.kube/cache")].into(),
        ..Default::default()
    };
    assert!(!touches(&paths, &unrelated));
}

#[test]
fn test_kubeconfig_model_is_shared_while_held_and_dropped_after() {
    App::test((), |mut app| async move {
        app.add_singleton_model(KubeContextModel::new_for_test);
        // A path kubectl cannot read, so the model's one fetch finds no context.
        let key = vec![PathBuf::from("/nonexistent-kube-test/config")];
        let subscribe = |app: &mut App| {
            KubeContextModel::handle(app).update(app, |models, ctx| {
                models.subscribe(&key, Some(String::new()), ctx)
            })
        };

        let first = subscribe(&mut app);
        let second = subscribe(&mut app);
        assert_eq!(first.id(), second.id());

        let weak = first.downgrade();
        drop(first);
        drop(second);
        assert!(app.read(|ctx| weak.upgrade(ctx)).is_none());
    });
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
    assert!(touches(&paths, &edited_target));
}
