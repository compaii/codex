"""Root-only structural checks; no mount, unit or provider operations."""
import argparse
import json
import os
from pathlib import Path
import pwd
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import native_writable_storage as storage


@unittest.skipUnless(os.geteuid() == 0, 'requires the explicit host operator')
class OperatorChecks(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='native-storage-check-', dir='/run')
        self.base = Path(self.directory.name)
        self.base.chmod(0o711)
        self.addCleanup(self.directory.cleanup)

    def args(self, action='status', **changes):
        values = dict(action=action, base=self.base, name='own-work', uid=65534,
                      bytes=storage.MIN_BYTES, reserve_bytes=1024 * 1024 * 1024)
        values.update(changes)
        return argparse.Namespace(**values)

    def record(self):
        owner = pwd.getpwuid(65534)
        for name, mode in [('work', 0o711), ('images', 0o700), ('records', 0o700)]:
            path = self.base / name
            path.mkdir()
            path.chmod(mode)
        (self.base / '.operator.lock').touch(mode=0o600)
        value = dict(schema='codex.native-writable-storage/v1', uid=65534,
                     gid=owner.pw_gid, bytes=storage.MIN_BYTES,
                     reserve_bytes=1024 * 1024 * 1024,
                     filesystem_uuid='03890118-91ca-4aa0-96bc-a47ca1d037cc', phase='ready')
        path = self.base / 'records/own-work.json'
        storage.write_record(path, value)
        return path

    def test_private_parent_is_not_opened_to_prepare_storage(self):
        self.base.chmod(0o700)
        with self.assertRaisesRegex(ValueError, 'traversal'), patch.object(storage, 'run') as run:
            storage.operate(self.args('prepare'))
        run.assert_not_called()
        self.assertEqual(self.base.stat().st_mode & 0o777, 0o700)
        self.assertFalse((self.base / 'images/own-work.ext4').exists())

    def test_directory_symlink_and_mutable_parents_are_refused(self):
        target = self.base / 'private'
        target.mkdir(mode=0o700)
        link = self.base / 'alias'
        link.symlink_to(target, target_is_directory=True)
        with self.assertRaises(ValueError):
            storage.directory(link, 0o711)
        self.base.chmod(0o777)
        with self.assertRaises(ValueError):
            storage.directory(target, 0o711)
        self.assertEqual(target.stat().st_mode & 0o777, 0o700)

    def test_regular_file_checks_preserve_private_bytes(self):
        private = self.base / 'private'
        private.write_bytes(b'original')
        private.chmod(0o600)
        alias = self.base / 'alias'
        alias.symlink_to(private)
        with self.assertRaises(ValueError):
            storage.regular(alias)
        alias.unlink()
        os.link(private, alias)
        with self.assertRaises(ValueError):
            storage.regular(private)
        self.assertEqual(private.read_bytes(), b'original')

    def test_invalid_names_and_capacities_cannot_invoke_host_tools(self):
        for changes in [dict(name='../other'), dict(name='other\nunit'),
                        dict(bytes=0), dict(bytes=storage.MAX_BYTES + 1)]:
            with self.subTest(changes=changes), patch.object(storage, 'run') as run:
                with self.assertRaises(ValueError):
                    storage.operate(self.args('prepare', **changes))
                run.assert_not_called()

    def test_existing_selection_cannot_be_resized_or_reassigned(self):
        path = self.record()
        before = path.read_bytes()
        with patch.object(storage, 'run', return_value=subprocess.CompletedProcess([], 0, 'unused.mount\n')) as run:
            with self.assertRaisesRegex(ValueError, 'selection differs'):
                storage.operate(self.args(bytes=storage.MIN_BYTES * 2))
        self.assertEqual(run.call_count, 1)  # Unit-name calculation, no mutation.
        self.assertEqual(path.read_bytes(), before)

    def test_status_preserves_record_and_uses_no_mutating_command(self):
        path = self.record()
        fd = os.open(self.base / 'images/own-work.ext4', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as image:
            os.posix_fallocate(image.fileno(), 0, storage.MIN_BYTES)
        before = path.read_bytes(), path.stat().st_mtime_ns
        with patch.object(storage, 'run', return_value=subprocess.CompletedProcess([], 0, 'unused.mount\n')), \
                patch.object(storage, 'mounted_image', return_value=False):
            result = storage.operate(self.args())
        self.assertEqual(result['state'], 'inactive')
        self.assertFalse(result['native_profile_changed'])
        self.assertEqual((path.read_bytes(), path.stat().st_mtime_ns), before)

    def test_foreign_mounted_image_is_preserved(self):
        responses = [subprocess.CompletedProcess([], 0, '/dev/loop99\n'),
                     subprocess.CompletedProcess([], 0, json.dumps({'loopdevices': [
                         {'back-file': '/different/image.ext4'}]}))]
        with patch.object(storage, 'run', side_effect=responses):
            with self.assertRaisesRegex(ValueError, 'different mounted'):
                storage.mounted_image(self.base / 'work', self.base / 'own.ext4')

    def test_registered_workspace_with_missing_signature_is_never_reformatted(self):
        path = self.record()
        record = json.loads(path.read_text())
        record['phase'] = 'prepared'  # Interrupted publication cannot authorize erasure.
        storage.write_record(path, record)
        image = self.base / 'images/own-work.ext4'
        fd = os.open(image, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'r+b') as stream:
            os.posix_fallocate(stream.fileno(), 0, storage.MIN_BYTES)
            stream.write(b'retained bytes')
        units = self.base / 'units'
        units.mkdir()
        unit = units / 'own.mount'
        unit.write_text(storage.definition(image, self.base / 'work/own-work'))
        unit.chmod(0o644)
        def selected_path(value):
            return units if value == '/etc/systemd/system' else Path(value)
        responses = [subprocess.CompletedProcess([], 0, 'own.mount\n'),
                     subprocess.CompletedProcess([], 2, '')]
        with patch.object(storage, 'Path', side_effect=selected_path), \
                patch.object(storage, 'run', side_effect=responses), \
                patch.object(storage, 'format_fresh_image') as formatter:
            with self.assertRaisesRegex(ValueError, 'no formatting'):
                storage.operate(self.args('prepare'))
        formatter.assert_not_called()
        with image.open('rb') as stream:
            self.assertEqual(stream.read(14), b'retained bytes')


if __name__ == '__main__':
    unittest.main()
