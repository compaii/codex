use super::*;
use std::os::unix::fs::PermissionsExt;

#[test]
fn raw_actor_program_is_not_rewritten_or_pinned() {
    let mut command = vec!["/private/actor".to_string(), "argument".to_string()];
    let original = command.clone();
    let bootstrap = RuntimeBootstrap::prepare(&mut command, false).unwrap();
    assert_eq!(command, original);
    assert!(bootstrap.helper.is_none());
}

#[test]
fn hidden_helper_role_cannot_pin_an_arbitrary_program_or_invocation() {
    let runner = File::open("/proc/self/exe").unwrap();
    let executable = std::env::current_exe().unwrap();
    for command in [
        vec!["/bin/sh".to_string(), FS_HELPER_ARG.to_string()],
        vec![executable.display().to_string(), "actor-script".to_string()],
        vec![
            executable.display().to_string(),
            FS_HELPER_ARG.to_string(),
            "extra".to_string(),
        ],
    ] {
        assert!(pin_helper(&command, &executable, &runner).is_err());
    }
}

#[test]
fn only_exact_self_or_non_symlink_package_sibling_is_eligible() {
    let package = tempfile::tempdir().unwrap();
    let executable = package.path().join("codex");
    let sibling = package.path().join("codex-exec-server");
    for path in [&executable, &sibling] {
        fs::write(path, "test executable").unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }
    let runner = File::open(&executable).unwrap();
    for path in [&executable, &sibling] {
        let command = vec![path.display().to_string(), FS_HELPER_ARG.to_string()];
        assert!(pin_helper(&command, &executable, &runner).is_ok());
    }
    fs::remove_file(&sibling).unwrap();
    std::os::unix::fs::symlink(&executable, &sibling).unwrap();
    // An alias to the running inode is safe because pin_helper clones runner,
    // never follows the alias while opening the trusted executable.
    let command = vec![sibling.display().to_string(), FS_HELPER_ARG.to_string()];
    assert!(pin_helper(&command, &executable, &runner).is_ok());
    let external = tempfile::NamedTempFile::new().unwrap();
    fs::remove_file(&sibling).unwrap();
    std::os::unix::fs::symlink(external.path(), &sibling).unwrap();
    assert!(pin_helper(&command, &executable, &runner).is_err());
}

#[test]
fn bootstrap_mounts_are_last_read_only_and_keep_descriptors_owned() {
    let bootstrap = RuntimeBootstrap::prepare(&mut ["/bin/true".to_string()], false).unwrap();
    let descriptor = bootstrap.runner.as_raw_fd();
    let mut bwrap = BwrapArgs {
        args: [
            "bwrap",
            "--bind",
            "/dev",
            "/dev",
            "--",
            RUNNER_ALIAS,
            "--",
            "/bin/true",
        ]
        .map(str::to_string)
        .to_vec(),
        preserved_files: Vec::new(),
        synthetic_mount_targets: Vec::new(),
    };
    bootstrap.install(&mut bwrap).unwrap();
    let separator = bwrap.args.iter().position(|arg| arg == "--").unwrap();
    assert_eq!(
        &bwrap.args[separator - 2..separator],
        &["--remount-ro", RUNTIME_DIR]
    );
    assert_eq!(
        &bwrap.args[separator + 1..],
        &[RUNNER_ALIAS, "--", "/bin/true"]
    );
    assert_eq!(bwrap.preserved_files.len(), 1);
    assert_eq!(bwrap.preserved_files[0].as_raw_fd(), descriptor);
}

#[test]
fn explicit_reserved_mount_collision_is_rejected() {
    let bootstrap = RuntimeBootstrap::prepare(&mut ["/bin/true".to_string()], false).unwrap();
    let mut bwrap = BwrapArgs {
        args: ["bwrap", "--bind", "/tmp", RUNTIME_DIR, "--", RUNNER_ALIAS]
            .map(str::to_string)
            .to_vec(),
        preserved_files: Vec::new(),
        synthetic_mount_targets: Vec::new(),
    };
    assert!(bootstrap.install(&mut bwrap).is_err());
}
