//! Keep only trusted bootstrap executables reachable after private-home masks.
//!
//! No package directory is exposed and raw actor programs are never rewritten.
//! Descriptor ownership and old-bubblewrap verification reuse the launcher path.

use crate::bwrap::BwrapArgs;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

const RUNTIME_DIR: &str = "/dev/codex-runtime";
pub(crate) const RUNNER_ALIAS: &str = "/dev/codex-runtime/codex-linux-sandbox";
const HELPER_ALIAS: &str = "/dev/codex-runtime/codex-exec-server";
const CODE_MODE_HOST_ALIAS: &str = "/dev/codex-runtime/codex-code-mode-host";
const FS_HELPER_ARG: &str = "--codex-run-as-fs-helper";

pub(crate) struct RuntimeBootstrap {
    runner: File,
    helper: Option<File>,
    helper_alias: &'static str,
}

impl RuntimeBootstrap {
    pub(crate) fn prepare(command: &mut [String], helper_role: bool) -> io::Result<Self> {
        let runner = File::open("/proc/self/exe")?;
        let helper_alias = if is_code_mode_host(command) {
            CODE_MODE_HOST_ALIAS
        } else {
            HELPER_ALIAS
        };
        let helper = if helper_role {
            let executable = std::env::current_exe()?;
            let helper = pin_helper(command, &executable, &runner)?;
            command[0] = helper_alias.to_string();
            Some(helper)
        } else {
            None
        };
        Ok(Self {
            runner,
            helper,
            helper_alias,
        })
    }

    pub(crate) fn install(self, bwrap: &mut BwrapArgs) -> io::Result<()> {
        let separator = bwrap
            .args
            .iter()
            .position(|arg| arg == "--")
            .ok_or_else(|| invalid("missing bubblewrap command separator"))?;
        // This is a reserved namespace-only path, not a user-selectable mount.
        // In particular, a writable /dev ancestor must not install a conflicting
        // child mount that later overrides the immutable runtime directory.
        if bwrap.args[..separator]
            .iter()
            .any(|arg| Path::new(arg).starts_with(RUNTIME_DIR))
            || fs::symlink_metadata(RUNTIME_DIR).is_ok()
        {
            return Err(invalid("runtime bootstrap mount path is reserved"));
        }
        let mut mounts = vec!["--tmpfs".to_string(), RUNTIME_DIR.to_string()];
        let mut files = vec![(self.runner, RUNNER_ALIAS)];
        if let Some(helper) = self.helper {
            files.push((helper, self.helper_alias));
        }
        for (file, destination) in files {
            mounts.extend([
                "--ro-bind-fd".to_string(),
                file.as_raw_fd().to_string(),
                destination.to_string(),
            ]);
            bwrap.preserved_files.push(file);
        }
        // Mount last, after all ordinary policy roots and denials. The fresh
        // directory contains only these exact read-only executable mounts, and
        // remounting it read-only prevents replacement through writable ancestors.
        mounts.extend(["--remount-ro".to_string(), RUNTIME_DIR.to_string()]);
        bwrap.args.splice(separator..separator, mounts);
        Ok(())
    }
}

fn is_code_mode_host(command: &[String]) -> bool {
    command.len() == 3 && command[1] == "--listen" && command[2] == "grpc+stdio://"
}

fn pin_helper(command: &[String], executable: &Path, runner: &File) -> io::Result<File> {
    let code_mode = is_code_mode_host(command);
    if !code_mode && (command.len() != 2 || command[1] != FS_HELPER_ARG) {
        return Err(invalid("invalid trusted filesystem helper invocation"));
    }
    let requested = fs::canonicalize(&command[0])?;
    let executable = fs::canonicalize(executable)?;
    let sibling = executable
        .parent()
        .ok_or_else(|| invalid("runtime executable has no package directory"))?
        .join(if code_mode {
            "codex-code-mode-host"
        } else {
            "codex-exec-server"
        });
    // A hidden CLI flag is not authority to pin an arbitrary actor executable.
    // The filesystem helper may be this running binary; Code Mode must be its
    // literal non-symlinked host sibling. Never pin a directory or actor program.
    // Runtime paths come from trusted executor provisioning.
    let file = if !code_mode && requested == executable {
        runner.try_clone()?
    } else if requested == sibling && !fs::symlink_metadata(&sibling)?.is_symlink() {
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(sibling)?
    } else {
        return Err(invalid(
            "filesystem helper is not owned by this runtime package",
        ));
    };
    let metadata = file.metadata()?;
    let requested_metadata = fs::metadata(&requested)?;
    if !metadata.is_file()
        || (metadata.dev(), metadata.ino()) != (requested_metadata.dev(), requested_metadata.ino())
        || metadata.mode() & 0o111 == 0
    {
        return Err(invalid("filesystem helper executable identity changed"));
    }
    Ok(file)
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
#[path = "runtime_bootstrap_tests.rs"]
mod tests;
