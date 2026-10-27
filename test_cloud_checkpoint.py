"""Exercise cloud checkpoint ordering against an actual local bare Git repo."""
from pathlib import Path
import io
import os
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import sync
from test_sync import FakeCloud, SyncTests


class DurableGitTest(unittest.TestCase):
    def setUp(self):
        self.parent = (Path(__file__).resolve().parent / "test_artifacts").resolve()
        self.parent.mkdir(exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(prefix="cloud-check-", dir=self.parent)
        self.root = Path(self.tmp.name).resolve()
        if not self.root.is_relative_to(self.parent):
            raise RuntimeError("Unexpected test directory")
        self.bare = self.root / "remote.git"
        self.work = self.root / "work"
        self.work.mkdir()
        self.git(self.root, "init", "--bare", "--initial-branch=main", str(self.bare))
        self.git(self.work, "init", "--initial-branch=main")
        self.git(self.work, "config", "user.name", "Local sync test")
        self.git(self.work, "config", "user.email", "local-test@example.invalid")
        self.git(self.work, "remote", "add", "origin", str(self.bare))
        self.env = patch.dict(os.environ, {"BAMBU_PERSIST": "git", "BAMBU_STATE_BRANCH": "main"})
        self.env.start()
        self.private_guard = patch.object(sync, "verify_private_repository", return_value=None)
        self.private_guard.start()
        self.old_root = sync.ROOT
        sync.ROOT = self.work
        self.fixture = SyncTests(methodName="runTest")
        self.fixture.setUp()
        self.con = sync.connect(self.work / "ledger.sqlite3")

    def tearDown(self):
        self.con.close()
        self.fixture.tearDown()
        sync.ROOT = self.old_root
        self.private_guard.stop()
        self.env.stop()
        # TemporaryDirectory recursively removes only the verified child of the
        # explicitly designated in-workspace test-artifacts directory.
        resolved = Path(self.tmp.name).resolve()
        if resolved != self.root or not resolved.is_relative_to(self.parent):
            raise RuntimeError("Refusing to remove an unexpected test directory")
        self.tmp.cleanup()

    @staticmethod
    def git(cwd, *args):
        p = subprocess.run(["git", *args], cwd=cwd, capture_output=True, check=False)
        if p.returncode:
            raise AssertionError(f"git {' '.join(args[:2])} failed")
        return p.stdout

    def seed(self):
        job = self.fixture.job("done", 2, 100, "112233FF")
        sync.initialize(self.con, self.fixture.cfg, self.fixture.spools, [job])
        self.git(self.work, "add", "ledger.sqlite3")
        self.git(self.work, "commit", "-m", "Initial ledger")
        self.git(self.work, "push", "origin", "HEAD:refs/heads/main")

    def test_remote_pending_is_durable_before_write_and_recovers_after_timeout(self):
        self.seed()
        test = self

        class CheckingCloud(FakeCloud):
            def update_weight(self, spool, grams):
                if str(spool["id"]) == "1":
                    data = test.git(test.root, "--git-dir", str(test.bare), "show", "refs/heads/main:ledger.sqlite3")
                    observed_path = test.root / "observed.sqlite3"
                    observed_path.write_bytes(data)
                    observed = sqlite3.connect(observed_path)
                    try:
                        pending = observed.execute("SELECT COUNT(*) FROM operations WHERE spool_id='1' AND status='pending'").fetchone()[0]
                    finally:
                        observed.close()
                    if pending != 1:
                        raise AssertionError("Remote ledger was not persisted before cloud mutation")
                super().update_weight(spool, grams)

        cloud = CheckingCloud(self.fixture.spools)
        cloud.fail_after_write = True
        self.assertTrue(sync.push(self.con, cloud))
        self.assertEqual(cloud.calls.count(("1", 900)), 1)

        # A fresh runner starts from the remote checkpoint, rather than the
        # interrupted runner's local SQLite file.
        new_work = self.root / "fresh"
        self.git(self.root, "clone", "--branch", "main", str(self.bare), str(new_work))
        self.git(new_work, "config", "user.name", "Local sync test")
        self.git(new_work, "config", "user.email", "local-test@example.invalid")
        sync.ROOT = new_work
        fresh = sync.connect(new_work / "ledger.sqlite3")
        try:
            self.assertEqual(sync.push(fresh, cloud), [])
            self.assertEqual(cloud.calls.count(("1", 900)), 1)
            self.assertEqual(fresh.execute("SELECT COUNT(*) FROM operations WHERE status='pending'").fetchone()[0], 0)
        finally:
            fresh.close()

    def test_git_push_conflict_blocks_all_bambu_writes(self):
        self.seed()
        other = self.root / "other"
        self.git(self.root, "clone", "--branch", "main", str(self.bare), str(other))
        self.git(other, "config", "user.name", "Local sync test")
        self.git(other, "config", "user.email", "local-test@example.invalid")
        (other / "unrelated.txt").write_text("divergent commit", encoding="utf-8")
        self.git(other, "add", "unrelated.txt")
        self.git(other, "commit", "-m", "Advance remote")
        self.git(other, "push", "origin", "HEAD:refs/heads/main")
        cloud = FakeCloud(self.fixture.spools)
        self.assertTrue(sync.push(self.con, cloud))
        self.assertEqual(cloud.calls, [])

    def test_lost_private_guard_blocks_bambu_writes(self):
        self.seed()
        self.private_guard.stop()
        try:
            with patch.object(sync, "verify_private_repository", side_effect=sync.SyncError("private check failed")):
                cloud = FakeCloud(self.fixture.spools)
                self.assertTrue(sync.push(self.con, cloud))
                self.assertEqual(cloud.calls, [])
        finally:
            self.private_guard.start()


class PrivacyTest(unittest.TestCase):
    def test_public_repo_is_rejected(self):
        class Opener:
            def open(self, *args, **kwargs):
                return io.BytesIO(b'{"private":false,"full_name":"owner/repo"}')
        env = {"BAMBU_PERSIST": "git", "GITHUB_REPOSITORY": "owner/repo", "GH_STATE_TOKEN": "test-only"}
        with patch.dict(os.environ, env), patch.object(sync.request, "build_opener", return_value=Opener()):
            with self.assertRaises(sync.SyncError):
                sync.verify_private_repository()


if __name__ == "__main__":
    unittest.main()
