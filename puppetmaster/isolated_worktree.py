"""Per-job git worktrees for full-edit verbs (``isolate: true``).

Full-edit workers edit their ``cwd`` in place behind a clean-tree guard, so
two jobs on one checkout cannot run at once and a job launched against a
checkout someone is editing races them. An isolated job gets its own worktree
on a fresh ``pm/implement-*`` branch cut from HEAD, under the Puppetmaster
state dir. Its commits land on that branch for the caller to review and merge;
the worktree is never removed automatically, so unmerged work cannot be lost.

Uncommitted changes in the caller's checkout are not carried over: an
isolated job starts from HEAD.
"""

from __future__ import annotations

import os
import secrets
import subprocess
import time
from pathlib import Path
from typing import Any

# Heavy dependency folders that are gitignored and would otherwise be missing
# (or need a full reinstall) in a fresh worktree. Linked, never copied.
LINKED_DEPENDENCY_DIRS = ("node_modules", ".venv", "venv")
_LINK_SEARCH_DEPTH = 2


def _git(cwd: str, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", cwd, *args],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if completed.returncode != 0:
        raise ValueError(completed.stderr.strip() or f"git {' '.join(args)} failed")
    return completed.stdout.strip()


def _dependency_dirs(top: Path) -> list[Path]:
    found = []
    for depth in range(_LINK_SEARCH_DEPTH):
        pattern = "/".join(["*"] * depth + ["{name}"]) if depth else "{name}"
        for name in LINKED_DEPENDENCY_DIRS:
            for path in top.glob(pattern.format(name=name)):
                if path.is_dir() and not path.is_symlink():
                    found.append(path)
    return found


def _exclude_links(top: Path, linked: list[str]) -> None:
    """A dir-only ignore rule (``node_modules/``) does not match a symlink, so
    a worker's ``git add -A`` would commit the links. Anchor each one in the
    shared info/exclude; in the main checkout those paths are already ignored."""
    if not linked:
        return
    exclude = Path(_git(str(top), "rev-parse", "--git-common-dir"))
    if not exclude.is_absolute():
        exclude = top / exclude
    exclude = exclude / "info" / "exclude"
    exclude.parent.mkdir(parents=True, exist_ok=True)
    present = set(exclude.read_text(encoding="utf-8").splitlines()) if exclude.exists() else set()
    missing = [f"/{path}" for path in linked if f"/{path}" not in present]
    if missing:
        with exclude.open("a", encoding="utf-8") as handle:
            handle.write("".join(f"{line}\n" for line in missing))


def create_isolated_worktree(cwd: str, state_dir: Path) -> dict[str, Any]:
    """Create a worktree + branch for one job; return where the worker runs."""
    top = Path(_git(cwd, "rev-parse", "--show-toplevel")).resolve()
    rel = os.path.relpath(Path(cwd).resolve(), top)
    head = _git(str(top), "rev-parse", "HEAD")
    job = f"{time.strftime('%Y%m%d-%H%M%S')}-{secrets.token_hex(3)}"
    branch = f"pm/implement-{job}"
    worktree = Path(state_dir).resolve() / "worktrees" / job
    worktree.parent.mkdir(parents=True, exist_ok=True)
    _git(str(top), "worktree", "add", "-q", "-b", branch, str(worktree), head)

    linked = []
    for source in _dependency_dirs(top):
        target = worktree / source.relative_to(top)
        if target.exists() or not target.parent.is_dir():
            continue
        target.symlink_to(source, target_is_directory=True)
        linked.append(source.relative_to(top).as_posix())
    _exclude_links(top, linked)

    return {
        "cwd": str(worktree / rel) if rel != "." else str(worktree),
        "worktree": str(worktree),
        "branch": branch,
        "base": head,
        "repo": str(top),
        "linked": linked,
    }
