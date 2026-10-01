"""The release wait only passes once pip's simple index lists the version."""
import importlib.util
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import hermetic_env  # noqa: F401

spec = importlib.util.spec_from_file_location(
    "wait_for_pypi", Path(__file__).resolve().parent.parent / "scripts" / "wait_for_pypi.py")
wait_for_pypi = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wait_for_pypi)


class WaitForPyPITests(unittest.TestCase):
    def test_waits_until_the_version_is_listed(self):
        pages = [{"versions": ["1.27.36"], "files": []},
                 {"versions": ["1.27.36"], "files": [{"filename": "puppetmaster_ai-1.27.37-py3-none-any.whl"}]}]
        sleeps = []
        self.assertTrue(wait_for_pypi.wait("1.27.37", 60, fetch=lambda: pages.pop(0),
                                           sleep=sleeps.append, clock=lambda: 0))
        self.assertEqual(sleeps, [15])

    def test_times_out_and_survives_fetch_errors(self):
        def fetch():
            raise OSError("cdn hiccup")
        ticks = iter([0, 0, 100])
        self.assertFalse(wait_for_pypi.wait("9.9.9", 50, fetch=fetch, sleep=lambda s: None,
                                            clock=lambda: next(ticks)))

    def test_similar_versions_do_not_match(self):
        self.assertFalse(wait_for_pypi.listed(
            {"files": [{"filename": "puppetmaster_ai-1.27.370-py3-none-any.whl"}]}, "1.27.37"))


if __name__ == "__main__":
    unittest.main()
