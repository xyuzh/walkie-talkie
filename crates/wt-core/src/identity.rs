//! Single install identity: one Ed25519 key shared by iroh transport and token signing.

use std::io::{Read, Write};
use std::path::Path;

use anyhow::{Context, Result};
use iroh::SecretKey;
use wt_proto::NodeId;

use crate::paths;

/// Loaded identity. Holds the iroh `SecretKey` (which is the same key as the on-disk seed).
#[derive(Clone)]
pub struct Identity {
    secret: SecretKey,
}

impl Identity {
    pub fn secret_key(&self) -> &SecretKey {
        &self.secret
    }

    pub fn nodeid(&self) -> NodeId {
        // iroh PublicKey is a 32-byte ed25519 pubkey; expose as our NodeId.
        let bytes = self.secret.public().as_bytes().to_owned();
        NodeId(bytes)
    }

    /// Load the on-disk key. Errors if it doesn't exist.
    pub fn load() -> Result<Self> {
        let p = paths::secret_key_path();
        let bytes = read_seed(&p).with_context(|| format!("read secret key at {}", p.display()))?;
        Ok(Self {
            secret: SecretKey::from_bytes(&bytes),
        })
    }

    /// Create a fresh key on disk (mode 0600) plus the corresponding pubkey file. Idempotent: if
    /// the file already exists, this just loads it.
    pub fn load_or_create() -> Result<Self> {
        let sk_path = paths::secret_key_path();
        if sk_path.exists() {
            return Self::load();
        }
        paths::ensure_dirs()?;
        let secret = SecretKey::generate();
        let bytes = secret.to_bytes();
        write_seed(&sk_path, &bytes)
            .with_context(|| format!("write secret key at {}", sk_path.display()))?;
        // pub key file is convenience for the user
        let pub_path = paths::public_key_path();
        let pubkey = secret.public();
        let pub_hex = hex::encode(pubkey.as_bytes());
        std::fs::write(&pub_path, format!("{pub_hex}\n"))
            .with_context(|| format!("write public key at {}", pub_path.display()))?;
        Ok(Self { secret })
    }
}

fn read_seed(p: &Path) -> std::io::Result<[u8; 32]> {
    let mut f = std::fs::File::open(p)?;
    let mut buf = [0u8; 32];
    f.read_exact(&mut buf)?;
    Ok(buf)
}

fn write_seed(p: &Path, seed: &[u8; 32]) -> std::io::Result<()> {
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(p)?;
    f.write_all(seed)?;
    f.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::with_temp_home;

    #[test]
    fn load_or_create_is_idempotent_and_writes_pubkey() {
        with_temp_home("identity-idempotent", |_| {
            let first = Identity::load_or_create().unwrap();
            let second = Identity::load_or_create().unwrap();

            assert_eq!(first.nodeid(), second.nodeid());
            assert_eq!(
                std::fs::metadata(paths::secret_key_path()).unwrap().len(),
                32
            );

            let pub_text = std::fs::read_to_string(paths::public_key_path()).unwrap();
            assert_eq!(pub_text.trim(), first.nodeid().to_string());
        });
    }

    #[cfg(unix)]
    #[test]
    fn secret_key_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        with_temp_home("identity-perms", |_| {
            let _ = Identity::load_or_create().unwrap();
            let mode = std::fs::metadata(paths::secret_key_path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        });
    }

    #[test]
    fn load_rejects_truncated_secret_key() {
        with_temp_home("identity-truncated", |_| {
            paths::ensure_dirs().unwrap();
            std::fs::write(paths::secret_key_path(), [7u8; 31]).unwrap();

            assert!(Identity::load().is_err());
        });
    }
}
