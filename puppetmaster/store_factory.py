from __future__ import annotations

from pathlib import Path
from typing import Union

from puppetmaster.sqlite_store import SQLiteSwarmStore
from puppetmaster.store import SwarmStore
from puppetmaster.workspace_scope import assert_same_scope


def create_store(
    backend: str,
    state_dir: Union[Path, str],
    *,
    mode: str = "deferred",
) -> SwarmStore:
    """Build a coordination store.

    ``mode`` controls bootstrap for both backends:
    - ``deferred`` (default): construct only. Supervisor APIs such as
      ``create_job`` call ``ensure_schema``; dashboard listing must not
      rewrite a corrupt ``state.sqlite3`` on open.
    - ``ensure``: create dirs, DDL, migrate immediately.
    - ``attach``: read-only validation; never create or migrate.
    """
    if mode not in {"deferred", "ensure", "attach"}:
        raise ValueError(f"unsupported store mode: {mode}")
    # Attach reads of other project dirs are allowed; supervisor ensure of a
    # *different* root while frozen is the silent-swap footgun we refuse.
    if mode == "ensure":
        assert_same_scope(state_dir)
    if backend == "file":
        store = SwarmStore(state_dir)
        if mode == "ensure":
            store.init()
        elif mode == "attach":
            store.attach()
        return store
    if backend == "sqlite":
        store = SQLiteSwarmStore(state_dir)
        store._open_mode = mode
        if mode == "attach":
            store.attach()
        elif mode == "ensure":
            store.ensure_schema()
        elif mode == "deferred":
            pass
        else:
            raise ValueError(f"unsupported store mode: {mode}")
        return store
    raise ValueError(f"unsupported backend: {backend}")


def create_worker_store(backend: str, state_dir: Union[Path, str]) -> SwarmStore:
    """Open a store the way a worker process must: attach only."""
    return create_store(backend, state_dir, mode="attach")
