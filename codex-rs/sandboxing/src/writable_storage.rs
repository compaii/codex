//! Verify an operator-provisioned bounded filesystem; never allocate or resize it.

use std::fs;
use std::io;
use std::io::Read;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct ValidatedWritableStorage {
    pub root: PathBuf,
    max_bytes: u64,
    device: u64,
    inode: u64,
}

impl ValidatedWritableStorage {
    pub fn check_writable_root(&self, path: &Path) -> io::Result<()> {
        let canonical = fs::canonicalize(path)?;
        if path != self.root
            || canonical != self.root
            || fs::metadata(&canonical)?.dev() != self.device
        {
            return Err(io::Error::other(
                "writable grants require the stable storage mount root",
            ));
        }
        Ok(())
    }

    pub fn revalidate(&self) -> io::Result<()> {
        let current = validate_writable_storage(&self.root, self.max_bytes)?;
        if current.device != self.device || current.inode != self.inode {
            return Err(io::Error::other("bounded storage identity changed"));
        }
        Ok(())
    }
}

/// Every actor-writable path must subsequently pass `check_writable_root`.
/// Capacity is the entire filesystem, not free space or a directory estimate.
pub fn validate_writable_storage(
    root: &Path,
    max_bytes: u64,
) -> io::Result<ValidatedWritableStorage> {
    if max_bytes == 0 || !root.is_absolute() || fs::canonicalize(root)? != root {
        return Err(io::Error::other("canonical bounded storage root required"));
    }
    let parent = root
        .parent()
        .ok_or_else(|| io::Error::other("storage root cannot be /"))?;
    for ancestor in parent.ancestors() {
        let info = fs::symlink_metadata(ancestor)?;
        if !info.is_dir() || info.uid() != 0 || info.mode() & 0o022 != 0 {
            return Err(io::Error::other(
                "operator-protected storage parents required",
            ));
        }
    }
    let info = fs::metadata(root)?;
    if !info.is_dir() || info.dev() == fs::metadata(parent)?.dev() {
        return Err(io::Error::other("dedicated bounded filesystem required"));
    }
    let mut mounts = Vec::new();
    fs::File::open("/proc/self/mountinfo")?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut mounts)?;
    if mounts.len() > 1024 * 1024 {
        return Err(io::Error::other("mount observation capacity exceeded"));
    }
    validate_mounts(root, &mounts)?;
    let path = std::ffi::CString::new(root.as_os_str().as_bytes())?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: path is NUL terminated; stat points to writable storage of the
    // exact libc type. Successful statvfs initializes that complete value.
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful statvfs call above initialized the complete value.
    let stat = unsafe { stat.assume_init() };
    let capacity = u128::from(stat.f_blocks) * u128::from(stat.f_frsize);
    if capacity == 0 || capacity > u128::from(max_bytes) {
        return Err(io::Error::other(
            "filesystem exceeds selected storage ceiling",
        ));
    }
    Ok(ValidatedWritableStorage {
        root: root.to_path_buf(),
        max_bytes,
        device: info.dev(),
        inode: info.ino(),
    })
}

fn validate_mounts(root: &Path, mounts: &[u8]) -> io::Result<()> {
    let mut matches = 0;
    for line in mounts
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let encoded = line
            .split(|byte| *byte == b' ')
            .nth(4)
            .ok_or_else(|| io::Error::other("invalid mount observation"))?;
        let path = decode_mountinfo_path(encoded)?;
        if path == root {
            matches += 1;
        } else if path.starts_with(root) {
            return Err(io::Error::other("nested mount escapes storage accounting"));
        }
    }
    if matches != 1 {
        return Err(io::Error::other("unambiguous storage mount required"));
    }
    Ok(())
}

/// Shared Linux mountinfo escaping, including non-UTF8 path bytes.
pub fn decode_mountinfo_path(encoded: &[u8]) -> io::Result<PathBuf> {
    let mut decoded = Vec::new();
    let mut bytes = encoded.iter().copied();
    while let Some(byte) = bytes.next() {
        decoded.push(if byte == b'\\' {
            let digits: Vec<_> = bytes.by_ref().take(3).collect();
            match digits.as_slice() {
                b"040" => b' ',
                b"011" => b'\t',
                b"012" => b'\n',
                b"134" => b'\\',
                _ => return Err(io::Error::other("invalid mountinfo path escape")),
            }
        } else {
            byte
        });
    }
    let path = PathBuf::from(std::ffi::OsString::from_vec(decoded));
    if !path.is_absolute() {
        return Err(io::Error::other("mountinfo path is not absolute"));
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quota_root_rejects_nested_or_ambiguous_mounts_and_decodes_spaces() {
        let root = Path::new("/run/own storage");
        let row = b"2 1 7:0 / /run/own\\040storage rw - ext4 /dev/loop3 rw\n";
        validate_mounts(root, row).unwrap();
        let nested = [
            row.as_slice(),
            b"3 2 8:0 / /run/own\\040storage/escape rw - ext4 other rw\n",
        ]
        .concat();
        assert!(validate_mounts(root, &nested).is_err());
        assert!(validate_mounts(root, &[row.as_slice(), row.as_slice()].concat()).is_err());
        assert!(validate_mounts(root, b"").is_err());
        assert!(decode_mountinfo_path(b"/run/bad\\999").is_err());
    }

    #[test]
    fn ordinary_directory_and_symlink_cannot_claim_a_filesystem_budget() {
        let temp = tempfile::tempdir().unwrap();
        assert!(validate_writable_storage(temp.path(), u64::MAX).is_err());
        let link = temp.path().join("link");
        std::os::unix::fs::symlink(temp.path(), &link).unwrap();
        assert!(validate_writable_storage(&link, u64::MAX).is_err());
    }

    #[test]
    #[ignore = "requires an explicitly provisioned operator-owned 32MiB filesystem"]
    fn actual_operator_volume_when_selected() {
        let root = std::env::var_os("CODEX_TEST_STORAGE_ROOT")
            .expect("explicit operator volume selection");
        let root = PathBuf::from(root);
        let volume = validate_writable_storage(&root, 32 * 1024 * 1024).unwrap();
        volume.check_writable_root(&root).unwrap();
        volume.revalidate().unwrap();
        assert!(volume.check_writable_root(Path::new("/tmp")).is_err());
        assert!(validate_writable_storage(&root, 1024 * 1024).is_err());
    }
}
