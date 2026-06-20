//! Resolve `~/.wt/...` paths, honoring `WT_HOME` for tests.

use std::path::{Path, PathBuf};

/// Root directory for this `wt` install. Honors `WT_HOME` if set, else `~/.wt`.
pub fn home() -> PathBuf {
    if let Ok(p) = std::env::var("WT_HOME") {
        return PathBuf::from(p);
    }
    let base = directories::BaseDirs::new()
        .map(|d| d.home_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    base.join(".wt")
}

pub fn keys_dir() -> PathBuf {
    home().join("keys")
}

pub fn secret_key_path() -> PathBuf {
    keys_dir().join("id_ed25519")
}

pub fn public_key_path() -> PathBuf {
    keys_dir().join("id_ed25519.pub")
}

pub fn data_dir() -> PathBuf {
    home().join("data")
}

pub fn state_db_path() -> PathBuf {
    data_dir().join("state.db")
}

pub fn run_dir() -> PathBuf {
    home().join("run")
}

pub fn daemon_sock_path() -> PathBuf {
    run_dir().join("daemon.sock")
}

pub fn daemon_pid_path() -> PathBuf {
    run_dir().join("daemon.pid")
}

pub fn logs_dir() -> PathBuf {
    home().join("logs")
}

/// Root under which per-session workspaces live: `~/.wt/sessions/<group>/<session>`.
pub fn sessions_dir() -> PathBuf {
    home().join("sessions")
}

/// Provisioned workspace directory for one session (the spawned child's cwd).
pub fn session_dir(group: &str, session: &str) -> PathBuf {
    sessions_dir().join(group).join(session)
}

/// Create every directory we use under `home()`, with appropriate permissions on Unix.
pub fn ensure_dirs() -> std::io::Result<()> {
    for d in [
        keys_dir(),
        data_dir(),
        run_dir(),
        logs_dir(),
        sessions_dir(),
    ] {
        std::fs::create_dir_all(&d)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perm = std::fs::metadata(&d)?.permissions();
            perm.set_mode(0o700);
            std::fs::set_permissions(&d, perm)?;
        }
    }
    Ok(())
}

/// Wipe a file path; ignore "not found".
pub fn unlink_if_exists(p: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(p) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}
