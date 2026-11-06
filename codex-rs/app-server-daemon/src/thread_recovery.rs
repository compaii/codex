//! Removes stale snapshots before planned daemon replacements.

use std::io::ErrorKind;
use std::io::Write;
use std::path::PathBuf;
use std::time::SystemTime;

use anyhow::Context;
use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;

use crate::Daemon;

#[derive(Serialize, Deserialize)]
pub(crate) struct PendingMaintenance {
    pub(crate) process: crate::backend::RunningProcess,
    pub(crate) managed_bin: PathBuf,
    pub(crate) snapshot_identity: crate::managed_install::ExecutableIdentity,
    pub(crate) snapshot_modified: SystemTime,
}

fn maintenance_path(daemon: &Daemon) -> PathBuf {
    daemon
        .settings_file
        .with_file_name("maintenance-handoff.json")
}

pub(crate) fn load_maintenance(daemon: &Daemon) -> Result<Option<PendingMaintenance>> {
    match std::fs::read(maintenance_path(daemon)) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

pub(crate) fn save_maintenance(daemon: &Daemon, pending: &PendingMaintenance) -> Result<()> {
    let path = maintenance_path(daemon);
    let mut file = tempfile::NamedTempFile::new_in(path.parent().context("maintenance parent")?)?;
    file.write_all(&serde_json::to_vec(pending)?)?;
    file.as_file().sync_all()?;
    file.persist(path)?;
    Ok(())
}

pub(crate) fn discard_maintenance(daemon: &Daemon) -> Result<()> {
    match std::fs::remove_file(maintenance_path(daemon)) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

pub(crate) fn discard_pending(daemon: &Daemon) -> Result<()> {
    discard_maintenance(daemon)?;
    match std::fs::remove_file(daemon.recovery_file()?) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).context("failed to clear daemon recovery file"),
    }
}
