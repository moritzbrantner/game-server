use crate::{GameSimulation, LiveMatchHost, MatchHost, MatchId, MatchRuntime, RecoveryImage};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::task::spawn_blocking;

pub const HOST_RECOVERY_BUNDLE_VERSION: u8 = 1;

const MANIFEST_FILE_NAME: &str = "manifest";
const MANIFEST_MAGIC: &str = "GSHR";
const RECOVERY_FILE_SUFFIX: &str = ".recovery";
const MAX_MANIFEST_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatchHostRecoveryConfig {
    pub directory: PathBuf,
}

#[derive(Debug)]
pub struct PreparedMatchHost<S: GameSimulation> {
    pub(crate) host: MatchHost<S>,
    pub(crate) recovery: MatchHostRecoveryPlan,
}

/// Prepared dynamic host. The management handle can be retained before serving begins.
pub struct PreparedLiveMatchHost<S: GameSimulation> {
    pub(crate) host: LiveMatchHost<S>,
    pub(crate) recovery: MatchHostRecoveryPlan,
}
impl<S: GameSimulation> PreparedLiveMatchHost<S> {
    pub fn host(&self) -> LiveMatchHost<S> {
        self.host.clone()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MatchHostRecoveryPlan {
    pub(crate) directory: PathBuf,
    pub(crate) consume_on_start: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MatchHostRecoveryError {
    EmptyMatchSet,
    DuplicateMatch(MatchId),
    Host(String),
    Manifest(String),
    Match { id: MatchId, error: String },
    Io(String),
}

impl fmt::Display for MatchHostRecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyMatchSet => write!(formatter, "hosted recovery requires at least one match"),
            Self::DuplicateMatch(id) => {
                write!(
                    formatter,
                    "hosted recovery match {id} is configured more than once"
                )
            }
            Self::Host(error) => write!(formatter, "hosted recovery host error: {error}"),
            Self::Manifest(error) => write!(formatter, "hosted recovery manifest error: {error}"),
            Self::Match { id, error } => {
                write!(formatter, "hosted recovery for match {id} failed: {error}")
            }
            Self::Io(error) => write!(formatter, "hosted recovery I/O error: {error}"),
        }
    }
}

impl Error for MatchHostRecoveryError {}

pub async fn prepare_match_host_for_recovery<S: GameSimulation>(
    matches: Vec<(MatchId, S)>,
    max_matches: usize,
    reconnect_grace_ticks: u64,
    config: MatchHostRecoveryConfig,
) -> Result<PreparedMatchHost<S>, MatchHostRecoveryError> {
    spawn_blocking(move || {
        prepare_match_host_for_recovery_sync(
            matches,
            max_matches,
            reconnect_grace_ticks,
            config,
            false,
        )
    })
    .await
    .map_err(|error| {
        MatchHostRecoveryError::Io(format!("hosted recovery preparation task failed: {error}"))
    })?
}

/// Restore the bounded manifest match set through a consumer-owned simulation factory.
/// Fresh IDs are used only when no recovery bundle exists; retired IDs are never recreated.
/// The factory runs on a blocking worker and must reproduce each simulation's rules/configuration.
pub async fn prepare_live_match_host_for_recovery<S, F, E>(
    fresh_ids: Vec<MatchId>,
    mut factory: F,
    max_matches: usize,
    reconnect_grace_ticks: u64,
    config: MatchHostRecoveryConfig,
) -> Result<PreparedLiveMatchHost<S>, MatchHostRecoveryError>
where
    S: GameSimulation,
    F: FnMut(&MatchId) -> Result<S, E> + Send + 'static,
    E: fmt::Display,
{
    spawn_blocking(move || {
        MatchHost::<S>::new(max_matches)
            .map_err(|error| MatchHostRecoveryError::Host(error.to_string()))?;
        ensure_no_incomplete_consumption(&config.directory)?;
        let ids = match fs::metadata(&config.directory) {
            Ok(metadata) if metadata.is_dir() => read_manifest_ids(&config.directory, max_matches)?,
            Ok(_) => {
                return Err(MatchHostRecoveryError::Io(
                    "recovery path is not a directory".to_owned(),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => fresh_ids,
            Err(error) => return Err(io_error(error)),
        };
        if ids.len() > max_matches {
            return Err(MatchHostRecoveryError::Host(
                "match set exceeds host capacity".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        let mut matches = Vec::with_capacity(ids.len());
        for id in ids {
            if !unique.insert(id.clone()) {
                return Err(MatchHostRecoveryError::DuplicateMatch(id));
            }
            let simulation = factory(&id).map_err(|error| MatchHostRecoveryError::Match {
                id: id.clone(),
                error: error.to_string(),
            })?;
            matches.push((id, simulation));
        }
        let prepared = prepare_match_host_for_recovery_sync(
            matches,
            max_matches,
            reconnect_grace_ticks,
            config,
            true,
        )?;
        Ok(PreparedLiveMatchHost {
            host: LiveMatchHost::new(prepared.host),
            recovery: prepared.recovery,
        })
    })
    .await
    .map_err(|error| {
        MatchHostRecoveryError::Io(format!("live recovery preparation task failed: {error}"))
    })?
}

fn read_manifest_ids(
    directory: &Path,
    max_matches: usize,
) -> Result<Vec<MatchId>, MatchHostRecoveryError> {
    let manifest = read_manifest(directory)?;
    let mut ids = Vec::new();
    for value in manifest.lines().skip(1) {
        if ids.len() == max_matches {
            return Err(MatchHostRecoveryError::Manifest(
                "recovered match set exceeds host capacity".to_owned(),
            ));
        }
        ids.push(
            MatchId::new(value)
                .map_err(|error| MatchHostRecoveryError::Manifest(error.to_string()))?,
        );
    }
    let expected = ids.iter().cloned().collect();
    validate_manifest(&manifest, &expected)?;
    validate_bundle_entries(directory, &expected)?;
    Ok(ids)
}

fn prepare_match_host_for_recovery_sync<S: GameSimulation>(
    matches: Vec<(MatchId, S)>,
    max_matches: usize,
    reconnect_grace_ticks: u64,
    config: MatchHostRecoveryConfig,
    allow_empty: bool,
) -> Result<PreparedMatchHost<S>, MatchHostRecoveryError> {
    if matches.is_empty() && !allow_empty {
        return Err(MatchHostRecoveryError::EmptyMatchSet);
    }

    let mut expected_ids = BTreeSet::new();
    for (id, _) in &matches {
        if !expected_ids.insert(id.clone()) {
            return Err(MatchHostRecoveryError::DuplicateMatch(id.clone()));
        }
    }

    ensure_no_incomplete_consumption(&config.directory)?;
    let recovery_images = match fs::metadata(&config.directory) {
        Ok(metadata) if metadata.is_dir() => {
            Some(read_recovery_bundle(&config.directory, &expected_ids)?)
        }
        Ok(_) => {
            return Err(MatchHostRecoveryError::Io(format!(
                "{} exists but is not a directory",
                config.directory.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(io_error(error)),
    };
    let consume_on_start = recovery_images.is_some();
    let mut recovery_images = recovery_images.unwrap_or_default();

    let mut host = MatchHost::new(max_matches)
        .map_err(|error| MatchHostRecoveryError::Host(error.to_string()))?;
    for (id, simulation) in matches {
        let runtime = if consume_on_start {
            let image = recovery_images.remove(&id).ok_or_else(|| {
                MatchHostRecoveryError::Manifest(format!(
                    "recovery image for configured match {id} is missing"
                ))
            })?;
            MatchRuntime::restore_from_recovery(simulation, image).map_err(|error| {
                MatchHostRecoveryError::Match {
                    id: id.clone(),
                    error: error.to_string(),
                }
            })?
        } else {
            MatchRuntime::new_with_replay_capture(simulation, reconnect_grace_ticks)
        };
        host.insert(id, runtime)
            .map_err(|error| MatchHostRecoveryError::Host(error.to_string()))?;
    }

    Ok(PreparedMatchHost {
        host,
        recovery: MatchHostRecoveryPlan {
            directory: config.directory,
            consume_on_start,
        },
    })
}

pub(crate) fn consume_recovery_bundle(directory: &Path) -> Result<(), MatchHostRecoveryError> {
    ensure_no_incomplete_consumption(directory)?;
    let consumed = sibling_path(directory, ".consumed")?;
    fs::rename(directory, &consumed).map_err(io_error)?;
    sync_parent_directory(directory)?;
    fs::remove_dir_all(&consumed).map_err(io_error)?;
    sync_parent_directory(directory)
}

pub(crate) fn write_recovery_bundle(
    directory: &Path,
    images: &BTreeMap<MatchId, RecoveryImage>,
) -> Result<(), MatchHostRecoveryError> {
    if directory.exists() {
        return Err(MatchHostRecoveryError::Io(format!(
            "recovery bundle {} already exists",
            directory.display()
        )));
    }
    ensure_no_incomplete_consumption(directory)?;
    let temp = sibling_path(directory, ".tmp")?;
    if temp.exists() {
        fs::remove_dir_all(&temp).map_err(io_error)?;
    }
    fs::create_dir_all(&temp).map_err(io_error)?;

    let result = (|| {
        let manifest = encode_manifest(images.keys());
        write_synced_file(&temp.join(MANIFEST_FILE_NAME), manifest.as_bytes())?;
        for (id, image) in images {
            image
                .write_atomic(&temp.join(recovery_file_name(id)))
                .map_err(|error| MatchHostRecoveryError::Match {
                    id: id.clone(),
                    error: error.to_string(),
                })?;
        }
        sync_directory(&temp)?;
        fs::rename(&temp, directory).map_err(io_error)?;
        if let Err(error) = sync_parent_directory(directory) {
            let rollback = fs::rename(directory, &temp);
            if rollback.is_ok() {
                let _ = sync_parent_directory(directory);
            }
            return Err(MatchHostRecoveryError::Io(format!(
                "recovery bundle commit sync failed: {error}"
            )));
        }
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_dir_all(&temp);
    }
    result
}

fn read_recovery_bundle(
    directory: &Path,
    expected_ids: &BTreeSet<MatchId>,
) -> Result<BTreeMap<MatchId, RecoveryImage>, MatchHostRecoveryError> {
    let manifest = read_manifest(directory)?;
    validate_manifest(&manifest, expected_ids)?;
    validate_bundle_entries(directory, expected_ids)?;

    expected_ids
        .iter()
        .map(|id| {
            let image = RecoveryImage::read_file(&directory.join(recovery_file_name(id))).map_err(
                |error| MatchHostRecoveryError::Match {
                    id: id.clone(),
                    error: error.to_string(),
                },
            )?;
            Ok((id.clone(), image))
        })
        .collect()
}

fn read_manifest(directory: &Path) -> Result<String, MatchHostRecoveryError> {
    let manifest_path = directory.join(MANIFEST_FILE_NAME);
    let metadata = fs::metadata(&manifest_path).map_err(io_error)?;
    let manifest_len = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
    if manifest_len > MAX_MANIFEST_BYTES {
        return Err(MatchHostRecoveryError::Manifest(format!(
            "manifest size {manifest_len} exceeds limit {MAX_MANIFEST_BYTES}"
        )));
    }
    fs::read_to_string(&manifest_path).map_err(io_error)
}

fn validate_manifest(
    manifest: &str,
    expected_ids: &BTreeSet<MatchId>,
) -> Result<(), MatchHostRecoveryError> {
    let mut lines = manifest.lines();
    let expected_header = format!("{MANIFEST_MAGIC} {HOST_RECOVERY_BUNDLE_VERSION}");
    if lines.next() != Some(expected_header.as_str()) {
        return Err(MatchHostRecoveryError::Manifest(
            "missing or unsupported bundle header".to_owned(),
        ));
    }
    let actual = lines.map(str::to_owned).collect::<Vec<_>>();
    let expected = expected_ids
        .iter()
        .map(|id| id.as_str().to_owned())
        .collect::<Vec<_>>();
    if actual != expected {
        return Err(MatchHostRecoveryError::Manifest(format!(
            "configured matches {expected:?} do not match bundle matches {actual:?}"
        )));
    }
    Ok(())
}

fn validate_bundle_entries(
    directory: &Path,
    expected_ids: &BTreeSet<MatchId>,
) -> Result<(), MatchHostRecoveryError> {
    let mut expected = expected_ids
        .iter()
        .map(|id| OsString::from(recovery_file_name(id)))
        .collect::<BTreeSet<_>>();
    expected.insert(OsString::from(MANIFEST_FILE_NAME));

    let actual = fs::read_dir(directory)
        .map_err(io_error)?
        .map(|entry| entry.map(|entry| entry.file_name()).map_err(io_error))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if actual != expected {
        return Err(MatchHostRecoveryError::Manifest(format!(
            "bundle entries {actual:?} do not match expected entries {expected:?}"
        )));
    }
    Ok(())
}

fn encode_manifest<'a>(ids: impl Iterator<Item = &'a MatchId>) -> String {
    let mut manifest = format!("{MANIFEST_MAGIC} {HOST_RECOVERY_BUNDLE_VERSION}\n");
    for id in ids {
        manifest.push_str(id.as_str());
        manifest.push('\n');
    }
    manifest
}

fn recovery_file_name(id: &MatchId) -> String {
    format!("{}{RECOVERY_FILE_SUFFIX}", id.as_str())
}

fn ensure_no_incomplete_consumption(directory: &Path) -> Result<(), MatchHostRecoveryError> {
    let consumed = sibling_path(directory, ".consumed")?;
    match fs::metadata(&consumed) {
        Ok(_) => Err(MatchHostRecoveryError::Manifest(format!(
            "incomplete recovery consumption marker {} exists",
            consumed.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}

fn sibling_path(path: &Path, suffix: &str) -> Result<PathBuf, MatchHostRecoveryError> {
    let file_name = path.file_name().ok_or_else(|| {
        MatchHostRecoveryError::Io(format!(
            "recovery bundle path {} must have a final component",
            path.display()
        ))
    })?;
    let mut sibling_name = file_name.to_os_string();
    sibling_name.push(suffix);
    Ok(path.with_file_name(sibling_name))
}

fn write_synced_file(path: &Path, bytes: &[u8]) -> Result<(), MatchHostRecoveryError> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(io_error)?;
    file.write_all(bytes).map_err(io_error)?;
    file.sync_all().map_err(io_error)
}

fn sync_parent_directory(path: &Path) -> Result<(), MatchHostRecoveryError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    sync_directory(parent)
}

fn sync_directory(path: &Path) -> Result<(), MatchHostRecoveryError> {
    #[cfg(unix)]
    {
        File::open(path)
            .and_then(|directory| directory.sync_all())
            .map_err(io_error)?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn io_error(error: std::io::Error) -> MatchHostRecoveryError {
    MatchHostRecoveryError::Io(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_support::TestDirectory;
    use crate::{DemoSimulation, RECONNECT_TOKEN_BYTES, ReconnectToken};

    fn factory(_: &MatchId) -> Result<DemoSimulation, std::convert::Infallible> {
        Ok(DemoSimulation::new())
    }
    #[tokio::test]
    async fn live_recovery_uses_manifest_ids_and_restores_sessions_without_recreating_retired_ids()
    {
        let directory = TestDirectory::new();
        let bundle = directory.path().join("recovery");
        let id = MatchId::new("created-at-runtime").unwrap();
        let token = ReconnectToken([7; RECONNECT_TOKEN_BYTES]);
        let mut runtime = MatchRuntime::new_with_replay_capture(DemoSimulation::new(), 100);
        let lease = runtime.admit(token).unwrap();
        runtime.advance_tick().unwrap();
        runtime.freeze_for_recovery();
        write_recovery_bundle(
            &bundle,
            &BTreeMap::from([(id.clone(), runtime.recovery_image().unwrap())]),
        )
        .unwrap();
        let prepared = prepare_live_match_host_for_recovery(
            vec![MatchId::new("retired-default").unwrap()],
            factory,
            2,
            100,
            MatchHostRecoveryConfig {
                directory: bundle.clone(),
            },
        )
        .await
        .unwrap();
        assert!(prepared.recovery.consume_on_start);
        assert!(
            bundle.exists(),
            "preparation must not consume before transport binds"
        );
        let statuses = prepared.host.statuses().await;
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].id, id);
        assert_eq!(statuses[0].current_tick, 1);
        assert_eq!(statuses[0].active_players, 0);
        assert_eq!(statuses[0].occupied_player_slots, 1);
        let hosted = prepared.host.get(&id).unwrap();
        let restored = hosted
            .runtime
            .lock()
            .await
            .reconnect(token, ReconnectToken([8; RECONNECT_TOKEN_BYTES]))
            .unwrap();
        assert_eq!(restored.player_id, lease.player_id);
        assert!(restored.connection_epoch > lease.connection_epoch);
    }
    #[tokio::test]
    async fn live_recovery_accepts_empty_bundles_and_preserves_empty_membership() {
        let directory = TestDirectory::new();
        let bundle = directory.path().join("recovery");
        let fresh = prepare_live_match_host_for_recovery(
            Vec::new(),
            factory,
            1,
            100,
            MatchHostRecoveryConfig {
                directory: bundle.clone(),
            },
        )
        .await
        .unwrap();
        assert!(fresh.host.statuses().await.is_empty());
        assert!(!fresh.recovery.consume_on_start);
        write_recovery_bundle(&bundle, &BTreeMap::new()).unwrap();
        let recovered = prepare_live_match_host_for_recovery(
            vec![MatchId::new("retired").unwrap()],
            factory,
            1,
            100,
            MatchHostRecoveryConfig { directory: bundle },
        )
        .await
        .unwrap();
        assert!(recovered.host.statuses().await.is_empty());
        assert!(recovered.recovery.consume_on_start);
    }
    #[tokio::test]
    async fn live_recovery_rejects_corrupt_or_over_capacity_membership_before_factory_calls() {
        let directory = TestDirectory::new();
        let bundle = directory.path().join("recovery");
        std::fs::create_dir(&bundle).unwrap();
        for manifest in [
            "GSHR 1\nalpha\nbeta\n",
            "GSHR 1\nalpha\nalpha\n",
            "GSHR 2\n",
            "GSHR 1\n../escape\n",
        ] {
            std::fs::write(bundle.join("manifest"), manifest).unwrap();
            let result = prepare_live_match_host_for_recovery(
                Vec::new(),
                |_: &MatchId| -> Result<DemoSimulation, std::convert::Infallible> {
                    panic!("invalid manifest must be rejected before invoking factory")
                },
                1,
                100,
                MatchHostRecoveryConfig {
                    directory: bundle.clone(),
                },
            )
            .await;
            assert!(matches!(result, Err(MatchHostRecoveryError::Manifest(_))));
        }
    }

    fn ids() -> BTreeSet<MatchId> {
        ["alpha", "beta"]
            .into_iter()
            .map(|id| MatchId::new(id).unwrap())
            .collect()
    }

    #[test]
    fn manifest_is_versioned_sorted_and_exact() {
        let ids = ids();
        let manifest = encode_manifest(ids.iter());
        assert_eq!(manifest, "GSHR 1\nalpha\nbeta\n");
        assert_eq!(validate_manifest(&manifest, &ids), Ok(()));
    }

    #[test]
    fn manifest_rejects_missing_extra_or_reordered_matches() {
        let ids = ids();
        for manifest in [
            "GSHR 1\nalpha\n",
            "GSHR 1\nalpha\nbeta\ngamma\n",
            "GSHR 1\nbeta\nalpha\n",
        ] {
            assert!(matches!(
                validate_manifest(manifest, &ids),
                Err(MatchHostRecoveryError::Manifest(_))
            ));
        }
    }
}
