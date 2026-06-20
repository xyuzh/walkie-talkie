//! Core library for `wt`: identity, store, auth, transport, services.

pub mod auth;
pub mod framing;
pub mod harness;
pub mod identity;
pub mod paths;
pub mod store;
pub mod transport;
pub mod workspace;

pub mod services {
    pub mod msg;
}

pub use wt_proto as proto;

#[cfg(test)]
pub(crate) mod test_support {
    use std::path::Path;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    pub(crate) fn with_temp_home<T>(name: &str, f: impl FnOnce(&Path) -> T) -> T {
        let _guard = ENV_LOCK.lock().unwrap();
        let old_home = std::env::var_os("WT_HOME");
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path =
            std::env::temp_dir().join(format!("wt-test-{name}-{}-{nanos}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        std::env::set_var("WT_HOME", &path);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&path)));

        match old_home {
            Some(v) => std::env::set_var("WT_HOME", v),
            None => std::env::remove_var("WT_HOME"),
        }
        let _ = std::fs::remove_dir_all(&path);

        match result {
            Ok(v) => v,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
}
