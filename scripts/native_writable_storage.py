#!/usr/bin/env python3
"""Prepare, inspect or deactivate one bounded native workspace as its host operator.

Uses a reserved ext4 image and the existing system mount manager. Does not select
a native profile, grant tools, start inference, resize or delete retained work.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import pwd
import re
import signal
import stat
import subprocess
import sys
import uuid


MIN_BYTES = 32 * 1024 * 1024
MAX_BYTES = 16 * 1024 * 1024 * 1024


def run(argv, *, optional=False):
    result = subprocess.run(argv, capture_output=True, text=True, timeout=30)
    if not optional:
        result.check_returncode()
    return result


def directory(path, mode):
    if path.exists():
        info = path.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise ValueError('operator-protected directory required')
    else:
        directory(path.parent, 0o755)
        path.mkdir(mode=mode)
        path.chmod(mode)
    # Check existing ancestors too, including mountpoints, without opening them
    # to another user or following an actor-controlled symlink.
    if path != path.parent:
        directory(path.parent, 0o755)


def regular(path, *, private=True):
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_nlink != 1
            or info.st_mode & (0o077 if private else 0o022)):
        raise ValueError('unchanged operator-owned regular file required')
    return info


def write_record(path, record):
    temporary = path.with_suffix('.staging')
    with temporary.open('x') as stream:
        os.fchmod(stream.fileno(), 0o600)
        json.dump(record, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def definition(image, mount):
    return ('[Unit]\nDescription=Bounded native workspace\nBefore=local-fs.target\n'
            f'[Mount]\nWhat={image}\nWhere={mount}\nType=ext4\n'
            'Options=loop,nodev,nosuid\nDirectoryMode=0700\nTimeoutSec=15\n'
            'ForceUnmount=no\nLazyUnmount=no\n[Install]\nWantedBy=local-fs.target\n')


def format_fresh_image(image, filesystem_uuid):
    # mke2fs normally discards backing-file extents, undoing fallocate's reserve.
    run(['mkfs.ext4', '-q', '-F', '-m', '0', '-E', 'nodiscard',
         '-U', filesystem_uuid, str(image)])


def mounted_image(mount, image):
    observed = run(['findmnt', '-n', '-o', 'SOURCE', '--mountpoint', str(mount)], optional=True)
    if observed.returncode == 1:
        return False
    observed.check_returncode()
    source = observed.stdout.strip()
    loops = json.loads(run(['losetup', '--json', '--list', source]).stdout)['loopdevices']
    if len(loops) != 1 or loops[0]['back-file'] != str(image):
        raise ValueError('different mounted backing image; preserved')
    return True


def observe(record, image, mount, unit):
    backing = regular(image)
    if backing.st_size != record['bytes'] or backing.st_blocks * 512 < record['bytes']:
        raise ValueError('backing reservation unavailable; no readiness claim')
    capacity = None
    available = None
    active = mounted_image(mount, image)
    if active:
        tree = run(['findmnt', '--submounts', '--mountpoint', str(mount),
                    '--noheadings', '--output', 'TARGET', '--raw']).stdout.splitlines()
        if tree != [str(mount)]:
            raise ValueError('nested mounts cannot share the selected storage budget')
        info = mount.stat()
        fs = os.statvfs(mount)
        capacity = fs.f_blocks * fs.f_frsize
        available = fs.f_bavail * fs.f_frsize
        if (info.st_dev == mount.parent.stat().st_dev
                or info.st_uid != record['uid'] or stat.S_IMODE(info.st_mode) != 0o700
                or not 0 < capacity <= record['bytes']):
            raise ValueError('mounted workspace does not satisfy its selected bound/owner')
    return dict(state='ready' if active else 'inactive', root=str(mount),
                maxBytes=record['bytes'], capacityBytes=capacity, availableBytes=available,
                reservedBytes=backing.st_blocks * 512,
                uid=record['uid'], unit=unit, work_preserved=True, native_profile_changed=False)


def operate(args):
    if not sys.platform.startswith('linux') or os.geteuid() != 0 or args.uid <= 0:
        raise ValueError('host operator and an existing non-root actor required')
    owner = pwd.getpwuid(args.uid)
    if (not re.fullmatch(r'[a-z][a-z0-9-]{0,47}', args.name)
            or not args.base.is_absolute() or args.base == Path('/')
            or not re.fullmatch(r'/[A-Za-z0-9_./-]+', str(args.base))
            or args.base.resolve() != args.base
            or not MIN_BYTES <= args.bytes <= MAX_BYTES
            or args.bytes % (1024 * 1024) or args.reserve_bytes < 0):
        raise ValueError('canonical operator base, bounded name/capacity and host reserve required')
    if args.action != 'prepare' and not args.base.exists():
        raise ValueError('workspace has not been prepared')
    directory(args.base, 0o711)
    for name, mode in [('images', 0o700), ('records', 0o700), ('work', 0o711)]:
        if args.action == 'prepare':
            directory(args.base / name, mode)
    for parent in (args.base / 'work', *(args.base / 'work').parents):
        if not parent.stat().st_mode & 0o001:
            raise ValueError('existing actor traversal unavailable; private parents unchanged')
    image = args.base / 'images' / f'{args.name}.ext4'
    mount = args.base / 'work' / args.name
    record_file = args.base / 'records' / f'{args.name}.json'
    unit = run(['systemd-escape', '--path', '--suffix=mount', str(mount)]).stdout.strip()
    unit_file = Path('/etc/systemd/system') / unit
    expected_unit = definition(image, mount)
    intent = dict(schema='codex.native-writable-storage/v1', uid=args.uid,
                  gid=owner.pw_gid, bytes=args.bytes, reserve_bytes=args.reserve_bytes)
    lock_path = args.base / '.operator.lock'
    lock_flags = os.O_NOFOLLOW | os.O_CLOEXEC
    lock_flags |= os.O_RDONLY if args.action == 'status' else os.O_RDWR | os.O_CREAT
    fd = os.open(lock_path, lock_flags, 0o600)
    with os.fdopen(fd, 'r') as lock:
        regular(lock_path)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if record_file.exists():
            if regular(record_file).st_size > 16384:
                raise ValueError('workspace record exceeds bound')
            record = json.loads(record_file.read_text())
            if any(record.get(key) != value for key, value in intent.items()):
                raise ValueError('existing workspace selection differs; no resize or reassignment')
        elif args.action == 'prepare':
            if image.exists() or mount.exists() or unit_file.exists():
                raise ValueError('unowned existing workspace preserved')
            record = dict(intent, filesystem_uuid=str(uuid.uuid4()), phase='prepared')
            write_record(record_file, record)
        else:
            raise ValueError('workspace record unavailable')
        if unit_file.exists():
            regular(unit_file, private=False)
            if unit_file.read_text() != expected_unit:
                raise ValueError('different mount unit preserved')
        if args.action == 'prepare':
            if not image.exists():
                free = os.statvfs(image.parent)
                if free.f_bavail * free.f_frsize - args.bytes < args.reserve_bytes:
                    raise ValueError('host storage reserve would be crossed')
                fd = os.open(image, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(fd, 'wb') as stream:
                    os.posix_fallocate(stream.fileno(), 0, args.bytes)
                    os.fsync(stream.fileno())
            info = regular(image)
            if info.st_size != args.bytes:
                raise ValueError('existing image length differs; retained for inspection')
            missing = max(0, args.bytes - info.st_blocks * 512)
            if missing:
                free = os.statvfs(image.parent)
                if free.f_bavail * free.f_frsize - missing < args.reserve_bytes:
                    raise ValueError('host storage reserve would be crossed')
                fd = os.open(image, os.O_WRONLY | os.O_NOFOLLOW)
                with os.fdopen(fd, 'wb') as stream:
                    os.posix_fallocate(stream.fileno(), 0, args.bytes)
                    os.fsync(stream.fileno())
            fs_uuid = run(['blkid', '-p', '-s', 'UUID', '-o', 'value', str(image)], optional=True)
            if fs_uuid.returncode == 2 and record['phase'] == 'prepared':
                format_fresh_image(image, record['filesystem_uuid'])
            elif fs_uuid.returncode != 0 or fs_uuid.stdout.strip() != record['filesystem_uuid']:
                raise ValueError('existing filesystem identity differs; no formatting')
            if not mount.exists():
                mount.mkdir(mode=0o700)
                mount.chmod(0o700)
            if not unit_file.exists():
                with unit_file.open('x') as stream:
                    os.fchmod(stream.fileno(), 0o644)
                    stream.write(expected_unit)
                    stream.flush()
                    os.fsync(stream.fileno())
                run(['systemctl', 'daemon-reload'])
            # An external umount can precede the manager's mount-table event.
            # Clear only this exact inactive mount's stale state before start.
            if not mounted_image(mount, image):
                run(['systemctl', 'stop', unit])
            run(['systemctl', 'enable', '--now', unit])
            if not mounted_image(mount, image):
                raise ValueError('selected workspace mount unavailable')
            os.chown(mount, args.uid, owner.pw_gid)
            mount.chmod(0o700)
            record['phase'] = 'ready'
            write_record(record_file, record)
        elif args.action == 'deactivate':
            if mounted_image(mount, image):
                run(['umount', str(mount)])  # Busy refuses; no lazy/forced unmount.
            run(['systemctl', 'stop', unit])
            run(['systemctl', 'disable', unit])
            record['phase'] = 'inactive'
            write_record(record_file, record)
        return observe(record, image, mount, unit)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'status', 'deactivate'])
    parser.add_argument('--base', type=Path, required=True)
    parser.add_argument('--name', required=True)
    parser.add_argument('--uid', type=int, required=True)
    parser.add_argument('--bytes', type=int, required=True)
    parser.add_argument('--reserve-bytes', type=int, required=True)
    args = parser.parse_args()
    signal.alarm(180)
    try:
        print(json.dumps(operate(args)))
    finally:
        signal.alarm(0)


if __name__ == '__main__':
    main()
