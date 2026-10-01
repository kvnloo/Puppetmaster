"""Block until pip's index serves a just-uploaded puppetmaster-ai version.

The project JSON page updates first; pip reads the simple index through a CDN
that can lag for minutes. Pin bumps that race it fail with "No matching
distribution found". Exit 0 once the version's files are listed, 1 on timeout.

    python scripts/wait_for_pypi.py 1.27.37 [--timeout 900]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request

INDEX = "https://pypi.org/simple/puppetmaster-ai/"


def fetch_index() -> dict:
    request = urllib.request.Request(
        INDEX, headers={"Accept": "application/vnd.pypi.simple.v1+json", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def listed(index: dict, version: str) -> bool:
    return version in index.get("versions", ()) or any(
        f"-{version}-" in f.get("filename", "") or f.get("filename", "").endswith(f"-{version}.tar.gz")
        for f in index.get("files", ()))


def wait(version: str, timeout: float, fetch=fetch_index, sleep=time.sleep, clock=time.monotonic) -> bool:
    deadline = clock() + timeout
    while True:
        try:
            if listed(fetch(), version):
                return True
        except OSError as exc:
            print(f"index fetch failed: {exc}", file=sys.stderr)
        if clock() >= deadline:
            return False
        sleep(15)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("version")
    parser.add_argument("--timeout", type=float, default=900)
    args = parser.parse_args(argv)
    if wait(args.version, args.timeout):
        print(f"puppetmaster-ai {args.version} is on the pip index")
        return 0
    print(f"puppetmaster-ai {args.version} not on the pip index after {args.timeout:.0f}s", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
