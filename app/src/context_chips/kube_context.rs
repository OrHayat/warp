//! App-wide `kubectl config current-context` values for local sessions.
//!
//! The Kubernetes chip's value only changes when a kubeconfig file changes, so local sessions
//! share one cached value per kubeconfig file list and refresh it from a filesystem watch
//! instead of running `kubectl` on every prompt.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use notify_debouncer_full::notify::{RecursiveMode, WatchFilter};
use warpui::r#async::SpawnedFutureHandle;
use warpui::{Entity, ModelContext, ModelHandle, SingletonEntity};
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

/// Returns the keys that read any file the event touched.
pub fn affected_keys<'a>(
    keys: impl IntoIterator<Item = &'a KubeconfigKey>,
    event: &BulkFilesystemWatcherEvent,
) -> Vec<KubeconfigKey> {
    let touched: HashSet<&PathBuf> = event
        .added
        .iter()
        .chain(&event.modified)
        .chain(&event.deleted)
        .chain(event.moved.keys())
        .chain(event.moved.values())
        .collect();
    keys.into_iter()
        .filter(|key| key.iter().any(|file| touched.contains(file)))
        .cloned()
        .collect()
}

pub enum KubeContextEvent {
    /// The current context for this kubeconfig was fetched or changed.
    Changed(KubeconfigKey),
}

struct Entry {
    /// `None` until the first fetch finishes; `Some(None)` when there is no current context.
    context: Option<Option<String>>,
    /// `PATH` used to find `kubectl`, from the session that registered the key.
    path_env: Option<String>,
    fetch: Option<SpawnedFutureHandle>,
}

pub struct KubeContextModel {
    entries: HashMap<KubeconfigKey, Entry>,
    watched_dirs: HashSet<PathBuf>,
    watcher: ModelHandle<BulkFilesystemWatcher>,
}

impl KubeContextModel {
    pub fn new(ctx: &mut ModelContext<Self>) -> Self {
        let watcher =
            ctx.add_model(|ctx| BulkFilesystemWatcher::new(KUBECONFIG_WATCHER_DEBOUNCE, ctx));
        Self::with_watcher(watcher, ctx)
    }

    pub fn new_for_test(ctx: &mut ModelContext<Self>) -> Self {
        let watcher = ctx.add_model(|_| BulkFilesystemWatcher::new_for_test());
        Self::with_watcher(watcher, ctx)
    }

    fn with_watcher(
        watcher: ModelHandle<BulkFilesystemWatcher>,
        ctx: &mut ModelContext<Self>,
    ) -> Self {
        ctx.subscribe_to_model(&watcher, Self::handle_fs_event);
        Self {
            entries: HashMap::new(),
            watched_dirs: HashSet::new(),
            watcher,
        }
    }

    /// The cached current context: `None` while unknown, `Some(None)` when there is none.
    pub fn current_context(&self, key: &KubeconfigKey) -> Option<Option<&str>> {
        let context = self.entries.get(key)?.context.as_ref()?;
        Some(context.as_deref())
    }

    /// Starts tracking `key`: watches its directories and fetches it once. A fetch that could
    /// not run (timeout, missing `kubectl`) is retried on the next call.
    pub fn ensure(
        &mut self,
        key: &KubeconfigKey,
        path_env: Option<String>,
        ctx: &mut ModelContext<Self>,
    ) {
        if let Some(entry) = self.entries.get_mut(key) {
            if entry.context.is_none() && entry.fetch.is_none() {
                entry.path_env = path_env.or(entry.path_env.take());
                self.fetch(key.clone(), ctx);
            }
            return;
        }
        for dir in key.iter().filter_map(|file| file.parent()) {
            if self.watched_dirs.insert(dir.to_path_buf()) {
                self.watcher.update(ctx, |watcher, _| {
                    // The directory, not the file: kubectl replaces the file by renaming.
                    std::mem::drop(watcher.register_path(
                        dir,
                        WatchFilter::accept_all(),
                        RecursiveMode::NonRecursive,
                    ));
                });
            }
        }
        self.entries.insert(
            key.clone(),
            Entry {
                context: None,
                path_env,
                fetch: None,
            },
        );
        self.fetch(key.clone(), ctx);
    }

    fn fetch(&mut self, key: KubeconfigKey, ctx: &mut ModelContext<Self>) {
        let Some(entry) = self.entries.get_mut(&key) else {
            return;
        };
        if let Some(handle) = entry.fetch.take() {
            handle.abort();
        }
        let files = key.clone();
        let path_env = entry.path_env.clone();
        entry.fetch = Some(ctx.spawn(
            async move { run_kubectl_current_context(files, path_env).await },
            move |me, result, ctx| {
                let Some(entry) = me.entries.get_mut(&key) else {
                    return;
                };
                entry.fetch = None;
                let Ok(context) = result else {
                    return;
                };
                if entry.context.as_ref() != Some(&context) {
                    entry.context = Some(context);
                    ctx.emit(KubeContextEvent::Changed(key));
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
        for key in affected_keys(self.entries.keys(), event) {
            self.fetch(key, ctx);
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
            log::debug!("KubeContextModel: failed to run kubectl: {err}");
            return Err(());
        }
        futures::future::Either::Right(_) => {
            log::debug!("KubeContextModel: kubectl timed out");
            return Err(());
        }
    };
    if !output.status.success() {
        return Ok(None);
    }
    let context = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    Ok((!context.is_empty()).then_some(context))
}

impl Entity for KubeContextModel {
    type Event = KubeContextEvent;
}

impl SingletonEntity for KubeContextModel {}

#[cfg(test)]
#[path = "kube_context_tests.rs"]
mod tests;
