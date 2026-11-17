//! Shared `kubectl config current-context` values for local sessions.
//!
//! The Kubernetes chip's value only changes when a kubeconfig file changes, so local sessions
//! share one value per kubeconfig file list, refreshed from a filesystem watch instead of
//! running `kubectl` on every prompt.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use notify_debouncer_full::notify::{RecursiveMode, WatchFilter};
use warpui::r#async::SpawnedFutureHandle;
use warpui::{Entity, ModelContext, ModelHandle, SingletonEntity, WeakModelHandle};
use watcher::{BulkFilesystemWatcher, BulkFilesystemWatcherEvent};

const KUBECONFIG_WATCHER_DEBOUNCE: Duration = Duration::from_millis(300);
const KUBECTL_TIMEOUT: Duration = Duration::from_secs(5);

/// The kubeconfig files a session reads, in `KUBECONFIG` order.
pub type KubeconfigKey = Vec<PathBuf>;

/// Returns the kubeconfig files `kubectl` would read: the `KUBECONFIG` path list, or
/// `<home>/.kube/config` when it is unset. Returns `None` for a list with a relative entry,
/// since that resolves against the shell's working directory.
pub fn kubeconfig_files(kubeconfig_env: Option<&str>, home: &Path) -> Option<KubeconfigKey> {
    let Some(value) = kubeconfig_env.filter(|value| !value.is_empty()) else {
        return Some(vec![home.join(".kube").join("config")]);
    };
    let files: KubeconfigKey = std::env::split_paths(value)
        .filter(|path| !path.as_os_str().is_empty())
        .collect();
    if files.is_empty() || files.iter().any(|path| path.is_relative()) {
        return None;
    }
    Some(files)
}

/// The paths whose changes affect `key`: each file, plus its target when it is a symlink.
pub fn watch_paths(key: &KubeconfigKey) -> Vec<PathBuf> {
    let mut paths = key.clone();
    for file in key {
        if let Ok(target) = std::fs::canonicalize(file)
            && !paths.contains(&target)
        {
            paths.push(target);
        }
    }
    paths
}

/// Whether the event touched any of `paths`.
pub fn touches(paths: &[PathBuf], event: &BulkFilesystemWatcherEvent) -> bool {
    event
        .added
        .iter()
        .chain(&event.modified)
        .chain(&event.deleted)
        .chain(event.moved.keys())
        .chain(event.moved.values())
        .any(|touched| paths.contains(touched))
}

/// Hands out one [`KubeconfigModel`] per kubeconfig. Callers hold the returned handle while
/// they show its context; the model, its `kubectl` run and its watch go away with the last one.
pub struct KubeContextModel {
    models: HashMap<KubeconfigKey, WeakModelHandle<KubeconfigModel>>,
    test_watchers: bool,
}

impl KubeContextModel {
    pub fn new(_: &mut ModelContext<Self>) -> Self {
        Self {
            models: HashMap::new(),
            test_watchers: false,
        }
    }

    /// Hands out models whose filesystem watchers have no background thread.
    pub fn new_for_test(_: &mut ModelContext<Self>) -> Self {
        Self {
            models: HashMap::new(),
            test_watchers: true,
        }
    }

    pub fn subscribe(
        &mut self,
        key: &KubeconfigKey,
        path_env: Option<String>,
        ctx: &mut ModelContext<Self>,
    ) -> ModelHandle<KubeconfigModel> {
        if let Some(model) = self.models.get(key).and_then(|weak| weak.upgrade(ctx)) {
            return model;
        }
        self.models.retain(|_, weak| weak.upgrade(ctx).is_some());
        let key = key.clone();
        let test_watchers = self.test_watchers;
        let model =
            ctx.add_model(|ctx| KubeconfigModel::new(key.clone(), path_env, test_watchers, ctx));
        self.models.insert(key, model.downgrade());
        model
    }
}

impl Entity for KubeContextModel {
    type Event = ();
}

impl SingletonEntity for KubeContextModel {}

pub enum KubeconfigEvent {
    /// The current context was fetched or changed.
    Changed,
}

/// The current context of one kubeconfig, refreshed when its files change.
pub struct KubeconfigModel {
    key: KubeconfigKey,
    /// `None` until a fetch succeeds; `Some(None)` when there is no current context.
    context: Option<Option<String>>,
    /// `PATH` used to find `kubectl`.
    path_env: Option<String>,
    /// From [`watch_paths`]; recomputed on each change, since a symlink may be retargeted.
    watch_paths: Vec<PathBuf>,
    watched_dirs: HashSet<PathBuf>,
    /// Kubeconfig directories that did not exist yet; an existing ancestor is watched instead.
    pending_dirs: HashSet<PathBuf>,
    watcher: ModelHandle<BulkFilesystemWatcher>,
    fetch: Option<SpawnedFutureHandle>,
}

impl KubeconfigModel {
    fn new(
        key: KubeconfigKey,
        path_env: Option<String>,
        test_watcher: bool,
        ctx: &mut ModelContext<Self>,
    ) -> Self {
        let watcher = ctx.add_model(|ctx| {
            if test_watcher {
                BulkFilesystemWatcher::new_for_test()
            } else {
                BulkFilesystemWatcher::new(KUBECONFIG_WATCHER_DEBOUNCE, ctx)
            }
        });
        ctx.subscribe_to_model(&watcher, Self::handle_fs_event);
        let mut model = Self {
            watch_paths: watch_paths(&key),
            key,
            context: None,
            path_env,
            watched_dirs: HashSet::new(),
            pending_dirs: HashSet::new(),
            watcher,
            fetch: None,
        };
        model.watch(ctx);
        model.fetch(ctx);
        model
    }

    pub fn key(&self) -> &KubeconfigKey {
        &self.key
    }

    /// The cached current context: `None` while unknown, `Some(None)` when there is none.
    pub fn current_context(&self) -> Option<Option<&str>> {
        Some(self.context.as_ref()?.as_deref())
    }

    /// Retries a fetch that could not run (timeout, `kubectl` not found on `PATH`).
    pub fn retry_if_unknown(&mut self, path_env: Option<String>, ctx: &mut ModelContext<Self>) {
        if self.context.is_none() && self.fetch.is_none() {
            self.path_env = path_env.or(self.path_env.take());
            self.fetch(ctx);
        }
    }

    /// Watches the directory of each path, not the file: kubectl replaces the file by renaming.
    fn watch(&mut self, ctx: &mut ModelContext<Self>) {
        for dir in self.watch_paths.iter().filter_map(|path| path.parent()) {
            let target = if dir.is_dir() {
                dir
            } else {
                self.pending_dirs.insert(dir.to_path_buf());
                match dir.ancestors().skip(1).find(|ancestor| ancestor.is_dir()) {
                    Some(ancestor) => ancestor,
                    None => continue,
                }
            };
            if self.watched_dirs.insert(target.to_path_buf()) {
                self.watcher.update(ctx, |watcher, _| {
                    std::mem::drop(watcher.register_path(
                        target,
                        WatchFilter::accept_all(),
                        RecursiveMode::NonRecursive,
                    ));
                });
            }
        }
    }

    fn fetch(&mut self, ctx: &mut ModelContext<Self>) {
        if let Some(handle) = self.fetch.take() {
            handle.abort();
        }
        let files = self.key.clone();
        let path_env = self.path_env.clone();
        self.fetch = Some(ctx.spawn(
            async move { run_kubectl_current_context(files, path_env).await },
            |me, result, ctx| {
                me.fetch = None;
                let Ok(context) = result else {
                    return;
                };
                if me.context.as_ref() != Some(&context) {
                    me.context = Some(context);
                    ctx.emit(KubeconfigEvent::Changed);
                }
            },
        ));
    }

    fn handle_fs_event(
        &mut self,
        _: ModelHandle<BulkFilesystemWatcher>,
        event: &BulkFilesystemWatcherEvent,
        ctx: &mut ModelContext<Self>,
    ) {
        let created_dir = self.pending_dirs.iter().any(|dir| dir.is_dir());
        self.pending_dirs.retain(|dir| !dir.is_dir());
        if !created_dir && !touches(&self.watch_paths, event) {
            return;
        }
        self.watch_paths = watch_paths(&self.key);
        self.watch(ctx);
        self.fetch(ctx);
    }
}

impl Entity for KubeconfigModel {
    type Event = KubeconfigEvent;
}

impl Drop for KubeconfigModel {
    fn drop(&mut self) {
        if let Some(handle) = self.fetch.take() {
            handle.abort();
        }
    }
}

/// `Ok(None)` when kubectl ran and reports no current context; `Err` when it could not run.
async fn run_kubectl_current_context(
    files: KubeconfigKey,
    path_env: Option<String>,
) -> Result<Option<String>, ()> {
    use command::Stdio;
    use command::r#async::Command;

    let kubeconfig = std::env::join_paths(&files).map_err(|_| ())?;
    let mut cmd = Command::new("kubectl");
    cmd.args(["config", "current-context"])
        .env("KUBECONFIG", kubeconfig)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    if let Some(path_env) = path_env {
        cmd.env("PATH", path_env);
    }

    let output = cmd.output();
    let timeout = async_io::Timer::after(KUBECTL_TIMEOUT);
    futures::pin_mut!(output);
    let output = match futures::future::select(output, timeout).await {
        futures::future::Either::Left((Ok(output), _)) => output,
        futures::future::Either::Left((Err(err), _)) => {
            log::debug!("KubeconfigModel: failed to run kubectl: {err}");
            return Err(());
        }
        futures::future::Either::Right(_) => {
            log::debug!("KubeconfigModel: kubectl timed out");
            return Err(());
        }
    };
    if !output.status.success() {
        return Ok(None);
    }
    let context = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    Ok((!context.is_empty()).then_some(context))
}

#[cfg(test)]
#[path = "kube_context_tests.rs"]
mod tests;
