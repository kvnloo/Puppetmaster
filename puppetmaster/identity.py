"""Store incarnation identity, independent of the path-derived state identity."""
from __future__ import annotations

import threading
from uuid import UUID, uuid4

from puppetmaster.models import JobRef
from puppetmaster.state import state_identity


class StoreIdentityError(ValueError):
    """Missing, corrupt, legacy or replaced identity; never implicitly rebind."""


class StoreMetadataDrift(StoreIdentityError):
    """Store metadata moved (ctime only) while a binding was in flight.

    Same device and inode, same size and mtime: nothing replaced or rewrote the
    store. macOS sets ``com.apple.provenance`` on a file the first time a new
    app lineage writes it, and chmod to the current mode does the same. A
    binding must not complete across the drift, but it may restart.
    """


_launch_prepare_lock = threading.Lock()
_prepared_launch_roots: set[str] = set()


def read_identity(c, backend):
    table = "metadata" if backend == "sqlite" else "projection_meta"
    row = c.execute(f"SELECT CASE WHEN length(CAST(value AS BLOB))=36 THEN value END FROM {table} WHERE key='incarnation'").fetchone()
    try:
        value = row[0] if row else None
        if str(UUID(value)) != value:
            raise ValueError()
    except (ValueError, TypeError, AttributeError) as exc:
        raise StoreIdentityError("store incarnation missing or corrupt; supervisor migration or explicit recovery required") from exc
    return value


def bootstrap(c, backend, *, legacy):
    table = "metadata" if backend == "sqlite" else "projection_meta"
    if legacy:
        c.execute(f"INSERT OR IGNORE INTO {table}(key,value) VALUES('incarnation',?)", (str(uuid4()),))
    return read_identity(c, backend)


def make_ref(root, job_id, incarnation):
    return JobRef(job_id, state_identity(root), version=2, incarnation=incarnation)


def legacy_schema(store, c):
    if store.backend_name != 'sqlite' or getattr(store, '_incarnation', None) is not None:
        return False
    row = c.execute("SELECT value='5' FROM metadata WHERE key='schema_version'").fetchone()
    return bool(row and row[0])


def scope_identity(store, ref, c):
    if ref is not None and ref.version == 1 and legacy_schema(store, c):
        return 'legacy:' + state_identity(store.root)
    return read_identity(c, store.backend_name)


def validate(store, ref, c, *, strict=False):
    if not isinstance(ref, JobRef) or ref.state_id != state_identity(store.root):
        raise StoreIdentityError("job_ref.state_id does not match this store")
    store._assert_safe_job_dir(ref.job_id)
    if ref.version == 1 and strict:
        raise StoreIdentityError("legacy JobRef cannot prove incarnation; explicitly rebind")
    if ref.version == 1 and legacy_schema(store, c):
        return ref
    incarnation = read_identity(c, store.backend_name)
    pinned = getattr(store, "_incarnation", None)
    if pinned is not None and pinned != incarnation:
        raise StoreIdentityError("store replaced since attach; explicitly reopen and rebind")
    if ref.version == 1 and strict:
        raise StoreIdentityError("legacy JobRef cannot prove incarnation; inspect this store and explicitly rebind with store.job_ref(job_id)")
    if ref.version == 2 and ref.incarnation != incarnation:
        raise StoreIdentityError("stale JobRef: store incarnation changed; inspect the replacement before rebinding")
    if ref.version == 2:
        store._incarnation = incarnation
        if store.backend_name == "sqlite" and not store._initialized:
            store._open_mode = "attach"
    return ref


def reference_at(root, job_id, *, expected_incarnation=None, launch_binding=False):
    from puppetmaster.store_factory import create_store
    store = create_store("sqlite" if (root / "state.sqlite3").exists() else "file", root)
    if expected_incarnation is not None:
        store._incarnation = expected_incarnation
    return store.job_ref(job_id, _launch_binding=launch_binding)


def prepare_launch(root, backend, job_ref=None):
    """Supervisor bootstrap before detaching; the child must attach this identity."""
    from puppetmaster.store_factory import create_store

    def prepare():
        store = create_store(backend, root)
        if job_ref is not None:
            store.bind_job_ref(job_ref)
        root_key = str(store.root.resolve())
        if (
            backend == "sqlite"
            and root_key in _prepared_launch_roots
            and store.db_path.exists()
        ):
            connection = store._connect_with_lock_retry()
            try:
                version = store._assert_schema(connection)
                if str(version) == str(store.schema_version):
                    store._incarnation = read_identity(connection, "sqlite")
                    return store._incarnation
            finally:
                connection.close()
        store.init()
        if backend == "sqlite":
            _prepared_launch_roots.add(root_key)
        # init captured this identity inside the supervisor transaction.
        # Reopening a metadata reader here races other launchers and their WAL.
        return store._incarnation

    if backend == "sqlite":
        # Detach calls can arrive concurrently in one MCP host. Ensure schema
        # once per root and process (including trigger refresh after upgrades);
        # followers validate identity without replaying DDL under a writer lock.
        with _launch_prepare_lock:
            return prepare()
    return prepare()
