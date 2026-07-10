//! Database lock file — prevents multiple OmniKV instances from opening
//! the same database directory simultaneously.
//!
//! This is critical for mmap soundness: if two processes open the same
//! directory, one could compact/rewrite SSTables while the other has them
//! memory-mapped, causing undefined behavior.
//!
//! Uses a two-layer locking strategy:
//! 1. **In-process lock**: A global `HashSet` of locked paths prevents
//!    two `OmniKV` instances in the same process from opening the same dir.
//! 2. **Cross-process lock**: OS-level file locking (flock on Unix,
//!    exclusive-open on Windows) prevents other processes.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Global set of currently locked database directories.
/// Prevents the same process from opening a database twice.
static LOCKED_DIRS: std::sync::LazyLock<Mutex<HashSet<PathBuf>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashSet::new()));

/// An exclusive file lock on the database directory.
///
/// While this lock is held, no other OmniKV instance (in this process
/// or any other) can open the same database directory.
pub struct LockFile {
    /// The open file handle — kept alive to hold the OS lock.
    _file: File,
    /// Canonical path to the database directory (for in-process tracking).
    dir_path: PathBuf,
}

impl LockFile {
    /// Acquires an exclusive lock on the given directory.
    ///
    /// Creates a `LOCK` file in the directory and acquires both an
    /// in-process lock and an OS-level file lock.
    pub fn acquire(dir: &Path) -> Result<Self, String> {
        // Ensure the directory exists
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("Failed to create database directory: {}", e))?;

        // Canonicalize the path so different relative paths to the same
        // directory are correctly identified as the same location.
        let canonical = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());

        // Layer 1: In-process lock
        {
            let mut locked = LOCKED_DIRS.lock().unwrap();
            if locked.contains(&canonical) {
                return Err(format!(
                    "Database is locked by another instance in this process (dir: {:?}). \
                     Only one OmniKV instance can open a database directory at a time.",
                    canonical
                ));
            }
            locked.insert(canonical.clone());
        }

        // Layer 2: OS-level file lock
        let lock_path = dir.join("LOCK");
        let file = match Self::open_and_lock(&lock_path) {
            Ok(f) => f,
            Err(e) => {
                // Release the in-process lock on failure
                let mut locked = LOCKED_DIRS.lock().unwrap();
                locked.remove(&canonical);
                return Err(e);
            }
        };

        // Write PID for diagnostics
        let mut wfile = file.try_clone().unwrap();
        let _ = wfile.set_len(0);
        let _ = write!(wfile, "{}", std::process::id());
        let _ = wfile.flush();

        Ok(Self {
            _file: file,
            dir_path: canonical,
        })
    }

    #[cfg(unix)]
    fn open_and_lock(lock_path: &Path) -> Result<File, String> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(lock_path)
            .map_err(|e| format!("Failed to open lock file {:?}: {}", lock_path, e))?;

        use std::os::unix::io::AsRawFd;
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            Err(format!(
                "Database is locked by another process (lock file: {:?}). \
                 Only one OmniKV instance can open a database directory at a time.",
                lock_path
            ))
        } else {
            Ok(file)
        }
    }

    #[cfg(windows)]
    fn open_and_lock(lock_path: &Path) -> Result<File, String> {
        // On Windows, we use share_mode(0) which opens the file with
        // exclusive access — no other process can open it simultaneously.
        // For same-process locking, we rely on the LOCKED_DIRS set above.
        use std::os::windows::fs::OpenOptionsExt;

        // Try to open with exclusive sharing (no other handles allowed)
        // FILE_SHARE_NONE = 0 means exclusive access
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .share_mode(0) // Exclusive — blocks other processes
            .open(lock_path)
            .map_err(|e| {
                format!(
                    "Database is locked by another process (lock file: {:?}): {}. \
                 Only one OmniKV instance can open a database directory at a time.",
                    lock_path, e
                )
            })?;

        Ok(file)
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        // Release the in-process lock
        if let Ok(mut locked) = LOCKED_DIRS.lock() {
            locked.remove(&self.dir_path);
        }
        // File handle closes automatically, releasing the OS lock
        // The LOCK file itself is left on disk (like LevelDB/RocksDB)
    }
}
