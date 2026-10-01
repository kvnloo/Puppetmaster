"""Win32 calls go through one typed kernel32 binding."""
import ast
import os
import subprocess
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import hermetic_env  # noqa: F401

PACKAGE = Path(__file__).resolve().parent.parent / "puppetmaster"


class Win32BindingTests(unittest.TestCase):
    def test_no_module_uses_the_shared_untyped_windll(self):
        # ctypes.windll.kernel32 is process-global and untyped: OpenProcess and
        # CreateToolhelp32Snapshot default to a 32-bit int return, truncating
        # 64-bit HANDLEs, and windll never captures GetLastError for
        # ctypes.get_last_error(). Every module used to roll its own copy.
        offenders = []
        for path in PACKAGE.rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                if (isinstance(node, ast.Attribute) and node.attr == "windll"
                        and isinstance(node.value, ast.Name) and node.value.id == "ctypes"):
                    offenders.append(f"{path.relative_to(PACKAGE.parent)}:{node.lineno}")
        self.assertEqual(offenders, [])

    @unittest.skipUnless(os.name == "nt", "Win32 liveness probe")
    def test_pid_alive_windows(self):
        from puppetmaster.win_process import pid_alive_windows
        self.assertTrue(pid_alive_windows(os.getpid()))
        child = subprocess.Popen([sys.executable, "-c", "pass"])
        child.wait()
        self.assertFalse(pid_alive_windows(child.pid))


if __name__ == "__main__":
    unittest.main()
