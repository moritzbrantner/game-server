use crate::{GameSimulation, MatchHost, MatchId, MatchRuntime, RecoveryImage};
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
                write!(formatter, "hosted recovery match {id} is configured more than once")
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
        )
    })
    .await
    .map_err(|error| {
        MatchHostRecoveryError::Io(format!("hosted recovery preparation task failed: {error}"))
    })?
}

fn prepare_match_host_for_recovery_sync<S: GameSimulation>(
    matches: Vec<(MatchId, S)>,
    max_matches: usize,
    reconnect_grace_ticks: u64,
    config: MatchHostRecoveryConfig,
) -> Result<PreparedMatchHost<S>, MatchHostRecoveryError> {
    if matches.is_empty() {
        return Err(MatchHostRecoveryError::EmptyMatchSet);
    }

    let mut expected_ids = BTreeSet::new();
    for (id, _) in &matches {
        if !expected_ids.insert(id.clone()) {
            return Err(MatchHostRecoveryError::DuplicateMatch(id.clone()));
        }
    }

    cleanup_consumed_bundle(&config.directory)?;
    let recovery_images = match fs::metadata(&config.directory) {
        Ok(metadata) if metadata.is_dir() => Some(read_recovery_bundle(
            &config.directory,
            &expected_ids,
        )?),
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
    let consumed = sibling_path(directory, ".consumed")?;
    if consumed.exists() {
        fs::remove_dir_all(&consumed).map_err(io_error)?;
    }
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
        sync_parent_directory(directory)
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
    let manifest_path = directory.join(MANIFEST_FILE_NAME);
    let metadata = fs::metadata(&manifest_path).map_err(io_error)?;
    let manifest_len = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
    if manifest_len > MAX_MANIFEST_BYTES {
        return Err(MatchHostRecoveryError::Manifest(format!(
            "manifest size {manifest_len} exceeds limit {MAX_MANIFEST_BYTES}"
        )));
    }
    let manifest = fs::read_to_string(&manifest_path).map_err(io_error)?;
    validate_manifest(&manifest, expected_ids)?;
    validate_bundle_entries(directory, expected_ids)?;

    expected_ids
        .iter()
        .map(|id| {
            let image = RecoveryImage::read_file(&directory.join(recovery_file_name(id)))
                .map_err(|error| MatchHostRecoveryError::Match {
                    id: id.clone(),
                    error: error.to_string(),
                })?;
            Ok((id.clone(), image))
        })
        .collect()
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

fn cleanup_consumed_bundle(directory: &Path) -> Result<(), MatchHostRecoveryError> {
    let consumed = sibling_path(directory, ".consumed")?;
    match fs::remove_dir_all(consumed) {
        Ok(()) => sync_parent_directory(directory),
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
