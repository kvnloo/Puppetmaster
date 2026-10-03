"""@cursor/sdk survives a reinstall of the Puppetmaster package.

Before: ``ensure_cursor_sdk`` installed the SDK beside the installed package
(site-packages). ``uv tool upgrade`` / ``pipx upgrade`` rebuild that
environment, so every upgrade silently deleted the SDK and the cursor adapter
failed with ``sdk_not_installed`` (Node: ERR_MODULE_NOT_FOUND) until someone
re-ran the bootstrap.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(__file__))
import hermetic_env  # noqa: F401

from puppetmaster import cursor_sdk_home
from puppetmaster.installers import ensure_cursor_sdk

REAL_RUNNER = cursor_sdk_home.PACKAGED_RUNNER

FAKE_SDK_INDEX = """
export class CursorAgentError extends Error {}
export class Agent {}
export const Cursor = { models: { list: async () => [{ id: "fake-model", displayName: "Fake" }] } };
"""
FAKE_SDK_SQLITE = "export class SqliteLocalAgentStore {}\n"


def _write_fake_sdk(prefix: Path) -> None:
    sdk = prefix / "node_modules" / "@cursor" / "sdk"
    sdk.mkdir(parents=True)
    (sdk / "package.json").write_text(json.dumps({
        "name": "@cursor/sdk", "version": "0.0.0-test", "type": "module",
        "exports": {".": "./index.js", "./sqlite": "./sqlite.js"},
    }), encoding="utf-8")
    (sdk / "index.js").write_text(FAKE_SDK_INDEX, encoding="utf-8")
    (sdk / "sqlite.js").write_text(FAKE_SDK_SQLITE, encoding="utf-8")


class CursorSdkHomeTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        tmp = Path(self._tmp.name)
        # A fresh install: the packaged runner has no node_modules above it.
        self.site = tmp / "site-packages" / "puppetmaster"
        self.site.mkdir(parents=True)
        self.runner = self.site / REAL_RUNNER.name
        shutil.copyfile(REAL_RUNNER, self.runner)
        self.state = tmp / "state"
        self._patches = [
            patch.object(cursor_sdk_home, "PACKAGED_RUNNER", self.runner),
            patch.dict(os.environ, {"PUPPETMASTER_APP_STATE_ROOT": str(self.state)}),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self) -> None:
        for p in reversed(self._patches):
            p.stop()
        self._tmp.cleanup()

    def test_bootstrap_installs_into_the_version_independent_home(self) -> None:
        calls = []

        def fake_npm(cmd, **kwargs):
            calls.append(cmd)
            _write_fake_sdk(Path(cmd[cmd.index("--prefix") + 1]))
            return SimpleNamespace(returncode=0, stdout="added 1 package", stderr="")

        with patch("puppetmaster.installers.subprocess.run", side_effect=fake_npm):
            result = ensure_cursor_sdk(npm_executable="/usr/bin/npm")
        self.assertEqual(result.status, "installed", result.detail)
        self.assertEqual(Path(calls[0][-1]), cursor_sdk_home.sdk_home())
        self.assertTrue(str(cursor_sdk_home.sdk_home()).startswith(str(self.state)))

    def test_workspace_node_modules_does_not_count_as_installed(self) -> None:
        with TemporaryDirectory() as workspace:
            _write_fake_sdk(Path(workspace))
            cwd = os.getcwd()
            os.chdir(workspace)
            try:
                with patch("puppetmaster.installers.shutil.which", return_value=None):
                    result = ensure_cursor_sdk()
            finally:
                os.chdir(cwd)
        self.assertEqual(result.status, "skipped", result.detail)

    def test_runner_stays_packaged_without_any_sdk(self) -> None:
        self.assertEqual(cursor_sdk_home.cursor_runner(), self.runner)

    def test_runner_runs_beside_the_home_sdk(self) -> None:
        _write_fake_sdk(cursor_sdk_home.sdk_home())
        runner = cursor_sdk_home.cursor_runner()
        self.assertEqual(runner.parent, cursor_sdk_home.sdk_home())
        self.assertEqual(runner.read_bytes(), REAL_RUNNER.read_bytes())
        # A changed packaged runner (upgrade) refreshes the copy.
        self.runner.write_bytes(REAL_RUNNER.read_bytes() + b"\n// upgraded\n")
        self.assertEqual(cursor_sdk_home.cursor_runner().read_bytes(), self.runner.read_bytes())

    @unittest.skipUnless(shutil.which("node"), "node is required")
    def test_node_resolves_the_home_sdk_after_a_reinstall(self) -> None:
        _write_fake_sdk(cursor_sdk_home.sdk_home())
        env = {**os.environ, "CURSOR_API_KEY": "test-key",
               "PUPPETMASTER_CURSOR_INPUT": json.dumps({"mode": "list-models"})}
        packaged = subprocess.run(["node", str(self.runner)], env=env,
                                  capture_output=True, text=True, timeout=60)
        self.assertNotEqual(packaged.returncode, 0)
        self.assertIn("ERR_MODULE_NOT_FOUND", packaged.stderr)
        resolved = subprocess.run(["node", str(cursor_sdk_home.cursor_runner())], env=env,
                                  capture_output=True, text=True, timeout=60)
        self.assertEqual(resolved.returncode, 0, resolved.stderr)
        self.assertEqual(json.loads(resolved.stdout)["models"][0]["id"], "fake-model")

    def test_cursor_adapter_dispatches_the_resolvable_runner(self) -> None:
        from puppetmaster.adapters import cursor as cursor_adapter

        _write_fake_sdk(cursor_sdk_home.sdk_home())
        self.assertIs(cursor_adapter.cursor_runner, cursor_sdk_home.cursor_runner)
        self.assertEqual(cursor_adapter.cursor_runner().parent, cursor_sdk_home.sdk_home())


if __name__ == "__main__":
    unittest.main()
