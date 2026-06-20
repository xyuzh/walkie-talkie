//! Per-session workspace provisioning (v3 orchestration).
//!
//! Each session runs its child harness in an isolated workspace under
//! `~/.wt/sessions/<group>/<session>` (the child's cwd), provisioned one of two ways:
//! - [`FsMode::Worktree`] — `git worktree add <ws> -b wt/<group>/<session>` off a base repo, so the
//!   child works on its own branch without touching the base checkout. Diffable and mergeable.
//! - [`FsMode::New`] — a fresh empty directory, for standing up a new component from scratch.
//!
//! Git is invoked as a subprocess (no libgit dependency); `git` must be on `PATH`.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{bail, Context, Result};
use tokio::process::Command;
use wt_proto::ipc::FsMode;

use crate::paths;

/// A provisioned session workspace.
#[derive(Debug, Clone)]
pub struct Workspace {
    /// Absolute path the child harness runs in (its cwd).
    pub path: PathBuf,
    /// Git branch backing the workspace, if any (worktree mode).
    pub branch: Option<String>,
}

/// Provision the workspace for `(group, session)` from `base_dir`, rooted at the standard
/// `~/.wt/sessions/<group>/<session>`. Thin wrapper over [`provision_at`].
pub async fn provision(
    group: &str,
    session: &str,
    base_dir: &Path,
    mode: FsMode,
) -> Result<Workspace> {
    let ws = paths::session_dir(group, session);
    let branch = format!("wt/{group}/{session}");
    provision_at(&ws, &branch, base_dir, mode).await
}

/// Provision a workspace at an explicit path. `branch` is used only in [`FsMode::Worktree`].
pub async fn provision_at(
    ws: &Path,
    branch: &str,
    base_dir: &Path,
    mode: FsMode,
) -> Result<Workspace> {
    if ws.exists() {
        bail!("workspace path already exists: {}", ws.display());
    }
    if let Some(parent) = ws.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create workspace parent {}", parent.display()))?;
    }
    match mode {
        FsMode::Worktree => {
            if !is_git_repo(base_dir).await {
                bail!(
                    "--worktree requires a git repository at {} (use --new for a fresh folder)",
                    base_dir.display()
                );
            }
            // `git worktree add` creates `ws` itself; its parent already exists (above).
            let out = Command::new("git")
                .arg("-C")
                .arg(base_dir)
                .arg("worktree")
                .arg("add")
                .arg(ws)
                .arg("-b")
                .arg(branch)
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .output()
                .await
                .context("spawn git worktree add")?;
            if !out.status.success() {
                bail!(
                    "git worktree add failed: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                );
            }
            Ok(Workspace {
                path: ws.to_path_buf(),
                branch: Some(branch.to_string()),
            })
        }
        FsMode::New => {
            std::fs::create_dir_all(ws)
                .with_context(|| format!("create workspace {}", ws.display()))?;
            Ok(Workspace {
                path: ws.to_path_buf(),
                branch: None,
            })
        }
    }
}

/// Tear down a session workspace. In worktree mode the worktree is pruned; the branch is **kept**
/// for merge-back unless `discard`. In new mode the directory is removed only when `discard`.
pub async fn teardown(
    ws: &Path,
    base_dir: Option<&Path>,
    mode: FsMode,
    branch: Option<&str>,
    discard: bool,
) -> Result<()> {
    match mode {
        FsMode::Worktree => {
            if let Some(base) = base_dir {
                // Force-remove ignores a dirty working tree (best-effort: the base repo may be gone).
                let _ = Command::new("git")
                    .arg("-C")
                    .arg(base)
                    .arg("worktree")
                    .arg("remove")
                    .arg("--force")
                    .arg(ws)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .await;
                if discard {
                    if let Some(b) = branch {
                        let _ = Command::new("git")
                            .arg("-C")
                            .arg(base)
                            .arg("branch")
                            .arg("-D")
                            .arg(b)
                            .stdout(Stdio::null())
                            .stderr(Stdio::null())
                            .status()
                            .await;
                    }
                }
            }
            if discard && ws.exists() {
                let _ = std::fs::remove_dir_all(ws);
            }
        }
        FsMode::New => {
            if discard && ws.exists() {
                std::fs::remove_dir_all(ws)
                    .with_context(|| format!("remove workspace {}", ws.display()))?;
            }
        }
    }
    Ok(())
}

async fn is_git_repo(dir: &Path) -> bool {
    if !dir.exists() {
        return false;
    }
    matches!(
        Command::new("git")
            .arg("-C")
            .arg(dir)
            .arg("rev-parse")
            .arg("--is-inside-work-tree")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await,
        Ok(s) if s.success()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let p = std::env::temp_dir().join(format!("wt-ws-{name}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    async fn run_git(dir: &Path, args: &[&str]) {
        let ok = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .unwrap()
            .success();
        assert!(ok, "git {args:?} failed");
    }

    async fn git_init_repo(dir: &Path) {
        std::fs::create_dir_all(dir).unwrap();
        run_git(dir, &["init", "-q"]).await;
        run_git(dir, &["config", "user.email", "t@t"]).await;
        run_git(dir, &["config", "user.name", "t"]).await;
        std::fs::write(dir.join("README.md"), b"hi").unwrap();
        run_git(dir, &["add", "-A"]).await;
        run_git(dir, &["commit", "-q", "-m", "init"]).await;
    }

    #[tokio::test]
    async fn new_mode_creates_fresh_dir_and_discards() {
        let base = tmp("new-base");
        let ws = tmp("new-ws");
        let w = provision_at(&ws, "wt/g/s", &base, FsMode::New)
            .await
            .unwrap();
        assert!(w.path.is_dir());
        assert!(w.branch.is_none());
        // Provisioning over an existing path is rejected.
        assert!(provision_at(&ws, "wt/g/s", &base, FsMode::New)
            .await
            .is_err());
        // Non-discard teardown keeps the folder; discard removes it.
        teardown(&ws, None, FsMode::New, None, false).await.unwrap();
        assert!(ws.exists());
        teardown(&ws, None, FsMode::New, None, true).await.unwrap();
        assert!(!ws.exists());
    }

    #[tokio::test]
    async fn worktree_mode_requires_git_and_keeps_branch_until_discard() {
        // A non-repo base is rejected.
        let nonrepo = tmp("nonrepo");
        std::fs::create_dir_all(&nonrepo).unwrap();
        let ws0 = tmp("ws0");
        assert!(provision_at(&ws0, "wt/g/s", &nonrepo, FsMode::Worktree)
            .await
            .is_err());

        // A real repo: the worktree is created on its branch and sees the committed file.
        let base = tmp("wt-base");
        git_init_repo(&base).await;
        let ws = tmp("wt-ws");
        let w = provision_at(&ws, "wt/myapp/frontend", &base, FsMode::Worktree)
            .await
            .unwrap();
        assert!(w.path.join("README.md").is_file());
        assert_eq!(w.branch.as_deref(), Some("wt/myapp/frontend"));

        let has_branch = |base: PathBuf| async move {
            let out = Command::new("git")
                .arg("-C")
                .arg(&base)
                .args(["branch", "--list", "wt/myapp/frontend"])
                .output()
                .await
                .unwrap();
            String::from_utf8_lossy(&out.stdout).contains("wt/myapp/frontend")
        };
        assert!(
            has_branch(base.clone()).await,
            "branch should exist after provision"
        );

        // Non-discard teardown prunes the worktree but keeps the branch.
        teardown(
            &ws,
            Some(&base),
            FsMode::Worktree,
            w.branch.as_deref(),
            false,
        )
        .await
        .unwrap();
        assert!(!ws.join("README.md").exists(), "worktree should be pruned");
        assert!(
            has_branch(base.clone()).await,
            "branch must survive a non-discard teardown (for merge-back)"
        );

        // Discard teardown also deletes the branch.
        teardown(
            &ws,
            Some(&base),
            FsMode::Worktree,
            w.branch.as_deref(),
            true,
        )
        .await
        .unwrap();
        assert!(
            !has_branch(base.clone()).await,
            "branch should be gone after discard"
        );

        let _ = std::fs::remove_dir_all(&base);
        let _ = std::fs::remove_dir_all(&nonrepo);
    }
}
