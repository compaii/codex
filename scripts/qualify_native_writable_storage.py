#!/usr/bin/env python3
"""Finite, model-free native storage qualification using a disposable 32MiB image.

Root provisions only this fixture; the selected test binary runs as its existing
non-root owner. Current/previous records are bounded and uncertain mounts refuse.
"""
import argparse
import json
import os
from pathlib import Path
import pwd
import stat
import selectors
import subprocess
import tempfile
import time


def command(argv):
    return subprocess.run(argv, check=True, capture_output=True, text=True, timeout=15).stdout


def write(path, value):
    temporary = path.with_suffix('.staging')
    with temporary.open('x') as stream:
        os.fchmod(stream.fileno(), 0o600)
        json.dump(value, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control-dir', type=Path, required=True)
    parser.add_argument('--test-binary', type=Path, required=True)
    parser.add_argument('--executor-binary', type=Path, required=True)
    parser.add_argument('--uid', type=int, required=True)
    parser.add_argument('--temporary-root', type=Path, required=True)
    args = parser.parse_args()
    if os.geteuid() != 0 or args.uid == 0:
        raise ValueError('root operator and existing non-root test owner required')
    owner = pwd.getpwuid(args.uid)
    runtime = Path('/run/user') / str(args.uid)
    runtime_info = runtime.lstat()
    if (not stat.S_ISDIR(runtime_info.st_mode) or runtime_info.st_uid != args.uid
            or runtime_info.st_mode & 0o077):
        raise ValueError('existing private own-user manager runtime required')
    control = args.control_dir
    info = control.lstat()
    if (not control.is_absolute() or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != 0 or info.st_mode & 0o077):
        raise ValueError('existing private operator control directory required')
    for executable in (args.test_binary, args.executor_binary):
        if not executable.is_absolute() or not executable.is_file():
            raise ValueError('explicit existing qualification binaries required')
    temporary_info = args.temporary_root.lstat()
    if (not stat.S_ISDIR(temporary_info.st_mode) or temporary_info.st_uid != args.uid
            or temporary_info.st_mode & 0o077
            or temporary_info.st_dev != args.test_binary.stat().st_dev):
        raise ValueError('existing private same-filesystem test staging required')
    state = control / 'native-journey.json'
    if state.exists():
        previous = json.loads(state.read_text())
        if not (previous.get('mount_and_loop_retired') and previous.get('image_retired')):
            raise ValueError('inspect retained uncertain fixture before another invocation')
        state.replace(control / 'native-journey.previous.json')
        log = control / 'native-journey.log'
        if log.exists():
            log.replace(control / 'native-journey.previous.log')
    image = control / 'native-storage.ext4'
    if image.exists():
        raise ValueError('existing fixture image preserved')
    public = Path(tempfile.mkdtemp(prefix='codex-storage-qualification-', dir='/run'))
    public.chmod(0o711)  # Explicit despite the operator's private umask.
    mount = public / 'work'
    mount.mkdir(mode=0o755)
    record = dict(schema='matrix.native-storage-journey/v1', started=time.time(),
        phase='prepared', models=0, provider_calls=0, host_filesystem_features_changed=False,
        mount=str(mount), image=str(image), image_bytes=32 * 1024 * 1024)
    write(state, record)
    try:
        fd = os.open(image, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.truncate(record['image_bytes'])
            stream.flush()
            os.fsync(stream.fileno())
        command(['/usr/sbin/mkfs.ext4', '-q', '-F', '-m', '0', str(image)])
        command(['mount', '-o', 'loop,nodev,nosuid,noexec', str(image), str(mount)])
        os.chown(mount, args.uid, owner.pw_gid)
        mount.chmod(0o700)
        record['phase'] = 'mounted'
        write(state, record)
        environment = dict(HOME=owner.pw_dir, PATH='/usr/bin:/bin', XDG_RUNTIME_DIR=str(runtime),
            CODEX_TEST_STORAGE_ROOT=str(mount), CODEX_TEST_EXEC_SERVER_BIN=str(args.executor_binary),
            RUST_MIN_STACK='8388608', TMPDIR=str(args.temporary_root))
        result = subprocess.Popen([str(args.test_binary), '--exact',
            'turn_process::tests::bounded_turn_reuses_verified_storage_for_raw_v8_and_private_temporary_files',
            '--ignored', '--nocapture'], user=args.uid, group=owner.pw_gid, extra_groups=[],
            cwd=str(args.test_binary.parent), env=environment,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 120
        encoded = bytearray()
        with selectors.DefaultSelector() as selected:
            selected.register(result.stdout, selectors.EVENT_READ)
            try:
                while True:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('finite native qualification deadline')
                    if not selected.select(timeout=min(1, deadline-time.monotonic())):
                        continue
                    chunk = result.stdout.read1(8192)
                    if not chunk:
                        break
                    if len(encoded)+len(chunk) > 1024*1024:
                        raise ValueError('native fixture output capacity exceeded')
                    encoded.extend(chunk)
                result.wait(timeout=max(0.1, deadline-time.monotonic()))
            finally:
                if result.poll() is None:
                    result.kill()
                    result.wait(timeout=5)
        output = encoded.decode(errors='replace')
        log = control / 'native-journey.log'
        log.write_bytes(encoded[-1024 * 1024:])
        log.chmod(0o600)
        record.update(phase='passed' if result.returncode == 0 else 'failed',
            exit=result.returncode, output_bytes=len(encoded), log_truncated=len(encoded)>1024*1024)
        write(state, record)
        print(output[-12000:])
    except Exception as error:
        record.update(phase='failed', error=str(error))
        raise
    finally:
        if os.path.ismount(mount):
            device = command(['findmnt', '-n', '-o', 'SOURCE', '--mountpoint', str(mount)]).strip()
            loops = json.loads(command(['losetup', '--json', '--list', device]))['loopdevices']
            if len(loops) != 1 or loops[0]['back-file'] != str(image):
                raise ValueError('foreign mount preserved')
            command(['umount', str(mount)])
            # Loop autoclear is asynchronous after the successful unmount. Observe
            # this exact backing file until released; never detach other devices.
            retirement_deadline = time.monotonic() + 5
            while True:
                loops = json.loads(command(['losetup', '--json', '--list']))['loopdevices']
                if not any(row.get('back-file') == str(image) for row in loops):
                    break
                if time.monotonic() >= retirement_deadline:
                    raise ValueError('owned loop retirement unconfirmed')
                time.sleep(0.05)
            record['mount_and_loop_retired'] = True
        if image.exists() and not os.path.ismount(mount):
            image.unlink()
            record['image_retired'] = True
        if not os.path.ismount(mount):
            mount.rmdir()
            public.rmdir()
            record['temporary_mount_directory_retired'] = True
        record['finished'] = time.time()
        write(state, record)
    print(json.dumps(record))
    return 0 if record['phase'] == 'passed' and not record['log_truncated'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
