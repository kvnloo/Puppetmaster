"""Opt-in per-job isolation for full-edit verbs.

Field report (2026-09-30): implement workers edit the caller's checkout in
place (behind a clean-tree guard), so three parallel workers needed worktrees
made by hand, and one launched against the live checkout would have raced the
caller's own edits. ``isolate`` gives each job its own worktree and branch.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from puppetmaster.isolated_worktree import create_isolated_worktree


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True).stdout.strip()


class IsolatedWorktreeTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self.repo = root / "repo"
        (self.repo / "webapp" / "node_modules" / "pkg").mkdir(parents=True)
        (self.repo / ".venv").mkdir()
        (self.repo / "webapp" / "app.ts").write_text("export {}\n")
        (self.repo / ".gitignore").write_text("node_modules/\n.venv/\n")
        _git(self.repo, "init", "-q")
        _git(self.repo, "add", "-A")
        _git(self.repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "init")
        self.state = root / "state"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_each_job_gets_its_own_branch_and_checkout_from_head(self) -> None:
        first = create_isolated_worktree(str(self.repo / "webapp"), self.state)
        second = create_isolated_worktree(str(self.repo / "webapp"), self.state)
        self.assertNotEqual(first["branch"], second["branch"])
        self.assertNotEqual(first["worktree"], second["worktree"])
        self.assertTrue(first["cwd"].endswith("webapp"))
        self.assertTrue((Path(first["cwd"]) / "app.ts").exists())
        self.assertEqual(_git(Path(first["worktree"]), "rev-parse", "HEAD"), _git(self.repo, "rev-parse", "HEAD"))
        self.assertEqual(_git(self.repo, "status", "--porcelain"), "")

    def test_ignored_dependency_dirs_are_linked_not_copied(self) -> None:
        made = create_isolated_worktree(str(self.repo), self.state)
        worktree = Path(made["worktree"])
        self.assertTrue(os.path.islink(worktree / "webapp" / "node_modules"))
        self.assertTrue((worktree / "webapp" / "node_modules" / "pkg").is_dir())
        self.assertTrue(os.path.islink(worktree / ".venv"))
        self.assertEqual(_git(worktree, "status", "--porcelain"), "")

    def test_refuses_outside_a_git_checkout(self) -> None:
        with self.assertRaises(ValueError):
            create_isolated_worktree(self._tmp.name, self.state)


if __name__ == "__main__":
    unittest.main()


class FullEditVerbIsolationTest(unittest.TestCase):
    def test_isolate_retargets_the_job_cwd_and_reports_the_branch(self) -> None:
        from puppetmaster.mcp_server import _full_edit_workspace

        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            repo.mkdir()
            (repo / "a.txt").write_text("a\n")
            _git(repo, "init", "-q")
            _git(repo, "add", "-A")
            _git(repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "init")
            args = {"cwd": str(repo), "isolate": True, "state_dir": str(Path(tmp) / "state")}
            self.assertIsNone(_full_edit_workspace(args))
            self.assertNotEqual(args["cwd"], str(repo))
            self.assertTrue(Path(args["cwd"], "a.txt").exists())
            self.assertTrue(args["_isolation"]["branch"].startswith("pm/implement-"))

    def test_isolate_outside_a_repo_is_a_clear_tool_error(self) -> None:
        from puppetmaster.mcp_server import _full_edit_workspace

        with tempfile.TemporaryDirectory() as tmp:
            result = _full_edit_workspace({"cwd": tmp, "isolate": True, "state_dir": str(Path(tmp) / "s")})
            self.assertTrue(result and result.get("isError"))
