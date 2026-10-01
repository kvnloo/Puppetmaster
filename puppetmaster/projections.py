"""Versioned metadata projection persistence; never read bodies on page paths.

SQLite source triggers make projections transactional. File stores use a local
SQLite metadata index with a durable pending-write marker. A crash between the
file rename and projection commit makes reads unavailable until explicit repair.
"""
from __future__ import annotations

import json
from puppetmaster.bounded_json import loads as metadata_json_loads
import secrets
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from puppetmaster.contracts import (
    CursorCodec, MAX_BYTES, MAX_PAGE, MAX_SCAN, MetadataPage, MetadataRef, TaskBinding,
    immutable_digest,
)
from puppetmaster.models import JobRef, to_jsonable
from puppetmaster.state import state_identity


def create_schema(c):
    if not c.in_transaction:
        c.execute("BEGIN IMMEDIATE")
    # Generated journal triggers must not reference a changing schema.
    for operation in ("INSERT", "UPDATE", "DELETE"):
        c.execute(f"DROP TRIGGER IF EXISTS projection_version_{operation}")
    c.execute("DROP TRIGGER IF EXISTS projection_previous_status")
    c.execute("DROP TRIGGER IF EXISTS projection_scope")
    c.execute("DROP TRIGGER IF EXISTS projection_journal_retention")
    c.execute("CREATE TABLE IF NOT EXISTS projection_meta(key TEXT PRIMARY KEY, value TEXT)")
    c.execute("INSERT OR IGNORE INTO projection_meta VALUES('secret', ?)", (secrets.token_hex(32),))
    c.execute("INSERT OR IGNORE INTO projection_meta VALUES('version', '1')")
    c.execute("INSERT OR IGNORE INTO projection_meta VALUES('epoch', '0')")
    c.execute("INSERT OR IGNORE INTO projection_meta VALUES('retention_floor', '0')")
    c.execute("""CREATE TABLE IF NOT EXISTS projection_changes(
        revision INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL,
        job_id TEXT NOT NULL, id TEXT NOT NULL, status TEXT, sha256 TEXT,
        stamp TEXT NOT NULL, deleted INTEGER NOT NULL DEFAULT 0,
        task_count INTEGER, artifact_count INTEGER, binding TEXT, task_id TEXT, artifact_type TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS projection_current(
        kind TEXT NOT NULL, job_id TEXT NOT NULL, id TEXT NOT NULL,
        status TEXT, sha256 TEXT, revision INTEGER NOT NULL, stamp TEXT NOT NULL,
        task_count INTEGER, artifact_count INTEGER, binding TEXT, task_id TEXT, artifact_type TEXT,
        PRIMARY KEY(kind, job_id, id))""")
    c.execute("CREATE INDEX IF NOT EXISTS projection_filter ON projection_current(kind,status,id)")
    c.execute("CREATE INDEX IF NOT EXISTS projection_scoped_filter ON projection_current(kind,job_id,status,id)")
    c.execute("CREATE INDEX IF NOT EXISTS projection_change_filter ON projection_changes(kind,status,revision)")
    c.execute("CREATE INDEX IF NOT EXISTS projection_order ON projection_current(kind,id)")
    c.execute("CREATE INDEX IF NOT EXISTS projection_feed ON projection_changes(kind, revision)")
    c.execute("CREATE INDEX IF NOT EXISTS projection_scoped_feed ON projection_changes(kind,job_id,revision)")
    c.execute("CREATE INDEX IF NOT EXISTS projection_entity_history ON projection_changes(kind,job_id,id,revision)")
    if "previous_status" not in {row[1] for row in c.execute("PRAGMA table_info(projection_changes)")}:
        c.execute("ALTER TABLE projection_changes ADD COLUMN previous_status TEXT")
        c.execute("""UPDATE projection_changes SET previous_status=(
            SELECT CASE WHEN prior.deleted=0 THEN prior.status END FROM projection_changes AS prior
            WHERE prior.kind=projection_changes.kind AND prior.job_id=projection_changes.job_id
              AND prior.id=projection_changes.id AND prior.revision<projection_changes.revision
            ORDER BY prior.revision DESC LIMIT 1)""")
    c.execute("CREATE INDEX IF NOT EXISTS projection_previous_filter ON projection_changes(kind,previous_status,revision)")
    c.execute("CREATE INDEX IF NOT EXISTS projection_change_scoped_filter ON projection_changes(kind,job_id,status,revision)")
    c.execute("CREATE INDEX IF NOT EXISTS projection_previous_scoped_filter ON projection_changes(kind,job_id,previous_status,revision)")
    # Both backends append the change before replacing the current projection.
    c.execute("""CREATE TRIGGER IF NOT EXISTS projection_previous_status
        AFTER INSERT ON projection_changes BEGIN
        UPDATE projection_changes SET previous_status=(
            SELECT status FROM projection_current
            WHERE kind=NEW.kind AND job_id=NEW.job_id AND id=NEW.id)
        WHERE revision=NEW.revision;
        END""")
    for table, columns in (("projection_current", ("scope",)),
                           ("projection_changes", ("scope", "previous_scope", "previous_membership"))):
        existing = {row[1] for row in c.execute(f"PRAGMA table_info({table})")}
        for column in columns:
            if column not in existing:
                c.execute(f"ALTER TABLE {table} ADD COLUMN {column} TEXT")
    c.execute("""CREATE TRIGGER IF NOT EXISTS projection_scope
        AFTER INSERT ON projection_changes BEGIN
        UPDATE projection_changes SET
            scope=COALESCE(NEW.scope, (SELECT scope FROM projection_current
                WHERE kind=NEW.kind AND job_id=NEW.job_id AND id=NEW.id)),
            previous_membership=CASE WHEN EXISTS(SELECT 1 FROM projection_current
                WHERE kind=NEW.kind AND job_id=NEW.job_id AND id=NEW.id) THEN 'present' ELSE 'absent' END,
            previous_scope=(SELECT scope FROM projection_current
                WHERE kind=NEW.kind AND job_id=NEW.job_id AND id=NEW.id)
        WHERE revision=NEW.revision;
        END""")
    from puppetmaster.job_display import install as install_display
    install_display(c)
    from puppetmaster.selected_economics import create_schema as install_economics
    install_economics(c)
    from puppetmaster.metadata_snapshot import install as install_snapshots
    install_snapshots(c)
    c.execute("""CREATE TRIGGER IF NOT EXISTS projection_journal_retention AFTER DELETE ON projection_changes BEGIN
        UPDATE projection_meta SET value=MAX(CAST(value AS INTEGER),OLD.revision) WHERE key='retention_floor';
        UPDATE projection_meta SET value=CAST(value AS INTEGER)+1 WHERE key='epoch'; END""")
    c.execute("CREATE TABLE IF NOT EXISTS projection_pending(path TEXT PRIMARY KEY)")
    from puppetmaster.completion_metadata import create_schema as create_completion_schema
    create_completion_schema(c)
    from puppetmaster.history_metadata import create_schema as create_history_schema
    create_history_schema(c)
    from puppetmaster.job_display import install_fact_touches
    install_fact_touches(c)
    c.execute("""CREATE TABLE IF NOT EXISTS contract_receipts(
        kind TEXT NOT NULL, job_id TEXT NOT NULL, id TEXT NOT NULL,
        data TEXT NOT NULL, PRIMARY KEY(kind,job_id,id))""")
    c.execute("""CREATE TABLE IF NOT EXISTS cancellation_targets(
        job_id TEXT, task_id TEXT, generation INTEGER, lease_id TEXT, owner TEXT,
        request_id TEXT, binding_digest TEXT, observed INTEGER,
        PRIMARY KEY(job_id,task_id,binding_digest,request_id))""")
    c.execute("CREATE INDEX IF NOT EXISTS cancellation_request ON cancellation_targets(job_id,request_id,observed)")


def install_source_triggers(c):
    if not c.in_transaction:
        c.execute("BEGIN IMMEDIATE")
    # Drop every dependent source trigger before ALTER TABLE validates them.
    # Rebuild in the same transaction, including on already-v5 databases.
    for table in ("jobs", "tasks", "artifacts"):
        for operation in ("INSERT", "UPDATE", "DELETE"):
            c.execute(f"DROP TRIGGER IF EXISTS projection_{table}_{operation}")
    create_schema(c)
    c.execute("DROP TRIGGER IF EXISTS selected_receipt_first_writer")
    c.execute("""CREATE TRIGGER selected_receipt_first_writer BEFORE UPDATE ON jobs
        WHEN json_extract(OLD.data,'$.status') IN ('complete','failed','cancelled')
          AND json_extract(NEW.data,'$.status') IN ('complete','failed','cancelled')
          AND json_extract(OLD.data,'$.cost_receipt') IS NOT NULL
          AND json_extract(NEW.data,'$.cost_receipt') IS NOT NULL
          AND (json_type(OLD.data,'$.cost_receipt') IS NOT json_type(NEW.data,'$.cost_receipt')
            OR json_extract(OLD.data,'$.cost_receipt') IS NOT json_extract(NEW.data,'$.cost_receipt'))
        BEGIN SELECT RAISE(ABORT,'conflicting frozen terminal receipt; reopen before refreezing'); END""")
    from puppetmaster.completion_metadata import install_source_triggers as install_receipts
    install_receipts(c)
    for table, kind in (("jobs", "job"), ("tasks", "task"), ("artifacts", "artifact")):
        job = "NEW.id" if kind == "job" else "NEW.job_id"
        old_job = "OLD.id" if kind == "job" else "OLD.job_id"
        status_key = "execution_status" if kind == "artifact" else "status"
        status = f"json_extract(NEW.data, '$.{status_key}')"
        binding = ("json_object('task_id',NEW.id,'generation',json_extract(NEW.data,'$.generation'),"
                   "'lease_id',json_extract(NEW.data,'$.lease_id'),'owner',json_extract(NEW.data,'$.lease_owner'))"
                   if kind == "task" else "NULL")
        task_id = "NEW.task_id" if kind == "artifact" else "NULL"
        artifact_type = "NEW.type" if kind == "artifact" else "NULL"
        sha = "json_extract(NEW.data, '$.sha256')" if kind == "artifact" else "NULL"
        legacy_sha = sha.replace("NEW.", "")
        scope = ("json_object('origin',json_extract(NEW.data,'$.origin'),"
                 "'project_id',json_extract(NEW.data,'$.project_id'),"
                 "'session_id',json_extract(NEW.data,'$.session_id'))" if kind == "job" else "NULL")
        # Seed missing projections without replacing existing revision provenance.
        c.execute(f"""INSERT OR IGNORE INTO projection_current
            (kind,job_id,id,status,sha256,revision,stamp,task_count,artifact_count,
             binding,task_id,artifact_type,scope)
            SELECT ?, {'id' if kind == 'job' else 'job_id'}, id,
                   json_extract(data, '$.{status_key}'),
                   {legacy_sha},
                   0, 'legacy_unknown', NULL, NULL,
                   {binding.replace('NEW.data','data').replace('NEW.id','id')},
                   {task_id.replace('NEW.','')}, {artifact_type.replace('NEW.','')},
                   {scope.replace('NEW.', '')} FROM {table}""", (kind,))
        if kind == "job":
            c.execute("""UPDATE projection_current SET scope=(SELECT
                json_object('origin',json_extract(data,'$.origin'),
                            'project_id',json_extract(data,'$.project_id'),
                            'session_id',json_extract(data,'$.session_id'))
                FROM jobs WHERE jobs.id=projection_current.id)
                WHERE kind='job' AND scope IS NULL""")
        from puppetmaster.job_display import FIELDS, sql_fields
        display_update = ("UPDATE projection_current SET " + ','.join(
            field + '=' + expr for field, expr in zip(FIELDS, sql_fields('NEW.data')))
            + " WHERE kind='job' AND job_id=NEW.id;" if kind == 'job' else '')
        from puppetmaster.selected_economics import source_sql
        economics_update = source_sql('NEW') if kind == 'job' else ''
        for operation in ("INSERT", "UPDATE"):
            child_touch = ""
            child_move = ""
            if kind != "job":
                column = "task_count" if kind == "task" else "artifact_count"
                delta = "1" if operation == "INSERT" else "(OLD.job_id != NEW.job_id)"
                if operation == "UPDATE":
                    child_move = f"""
                        INSERT INTO projection_changes(kind,job_id,id,status,sha256,stamp,deleted,binding,task_id,artifact_type)
                            SELECT kind,job_id,id,status,sha256,'known',1,binding,task_id,artifact_type
                            FROM projection_current WHERE kind='{kind}' AND job_id=OLD.job_id
                            AND id=OLD.id AND OLD.job_id != NEW.job_id;
                        DELETE FROM projection_current WHERE kind='{kind}' AND job_id=OLD.job_id
                            AND id=OLD.id AND OLD.job_id != NEW.job_id;
                        UPDATE projection_current SET {column}=MAX(0,COALESCE({column},0)-1)
                            WHERE kind='job' AND job_id=OLD.job_id AND OLD.job_id != NEW.job_id;
                        INSERT INTO projection_changes(kind,job_id,id,status,stamp,task_count,artifact_count)
                            SELECT kind,job_id,id,status,'known',task_count,artifact_count
                            FROM projection_current WHERE kind='job' AND job_id=OLD.job_id
                            AND OLD.job_id != NEW.job_id;
                        UPDATE projection_current SET revision=last_insert_rowid(), stamp='known'
                            WHERE kind='job' AND job_id=OLD.job_id AND OLD.job_id != NEW.job_id;
                    """
                child_touch = f"""
                    UPDATE projection_current SET {column}=COALESCE({column},0)+{delta}
                        WHERE kind='job' AND job_id={job};
                    INSERT INTO projection_changes(kind,job_id,id,status,stamp,task_count,artifact_count)
                        SELECT 'job',job_id,id,status,'known',task_count,artifact_count
                        FROM projection_current WHERE kind='job' AND job_id={job};
                    UPDATE projection_current SET revision=last_insert_rowid(), stamp='known'
                        WHERE kind='job' AND job_id={job};
                """
            c.execute(f"""CREATE TRIGGER IF NOT EXISTS projection_{table}_{operation}
                AFTER {operation} ON {table} BEGIN
                {child_move}
                INSERT INTO projection_changes(kind,job_id,id,status,sha256,stamp,scope)
                VALUES('{kind}',{job},NEW.id,{status},{sha},'known',{scope});
                INSERT INTO projection_current
                    (kind,job_id,id,status,sha256,revision,stamp,task_count,artifact_count,
                     binding,task_id,artifact_type,scope) VALUES(
                    '{kind}',{job},NEW.id,{status},{sha},last_insert_rowid(),'known',
                    {'0,0' if kind == 'job' else 'NULL,NULL'}, {binding}, {task_id}, {artifact_type}, {scope})
                ON CONFLICT(kind,job_id,id) DO UPDATE SET status=excluded.status,
                    sha256=excluded.sha256, revision=excluded.revision, stamp=excluded.stamp,
                    binding=excluded.binding, task_id=excluded.task_id, artifact_type=excluded.artifact_type,
                    scope=excluded.scope;
                UPDATE projection_changes SET
                    binding={binding}, task_id={task_id}, artifact_type={artifact_type},
                    task_count=(SELECT task_count FROM projection_current WHERE kind='{kind}' AND job_id={job} AND id=NEW.id),
                    artifact_count=(SELECT artifact_count FROM projection_current WHERE kind='{kind}' AND job_id={job} AND id=NEW.id)
                    WHERE revision=(SELECT revision FROM projection_current
                        WHERE kind='{kind}' AND job_id={job} AND id=NEW.id);
                {display_update}
                {economics_update}
                {child_touch}
                END""")
        child_delete = ""
        if kind != "job":
            column = "task_count" if kind == "task" else "artifact_count"
            child_delete = f"""
                UPDATE projection_current SET {column}=MAX(0,COALESCE({column},0)-1)
                    WHERE kind='job' AND job_id={old_job};
                INSERT INTO projection_changes(kind,job_id,id,status,stamp,task_count,artifact_count)
                    SELECT 'job',job_id,id,status,'known',task_count,artifact_count
                    FROM projection_current WHERE kind='job' AND job_id={old_job};
                UPDATE projection_current SET revision=last_insert_rowid(), stamp='known'
                    WHERE kind='job' AND job_id={old_job};
            """
        c.execute(f"""CREATE TRIGGER IF NOT EXISTS projection_{table}_DELETE
            AFTER DELETE ON {table} BEGIN
            INSERT INTO projection_changes(kind,job_id,id,status,sha256,stamp,deleted,task_count,artifact_count,binding,task_id,artifact_type)
            SELECT kind,job_id,id,status,sha256,'known',1,task_count,artifact_count,binding,task_id,artifact_type
            FROM projection_current WHERE kind='{kind}' AND job_id={old_job} AND id=OLD.id;
            DELETE FROM projection_current WHERE kind='{kind}' AND job_id={old_job} AND id=OLD.id;
            {child_delete}
            {"DELETE FROM selected_economics_current WHERE job_id=OLD.id;" if kind == 'job' else ''}
            END""")
    c.execute("""UPDATE projection_current SET
        task_count=(SELECT COUNT(*) FROM tasks WHERE tasks.job_id=projection_current.job_id),
        artifact_count=(SELECT COUNT(*) FROM artifacts WHERE artifacts.job_id=projection_current.job_id)
        WHERE kind='job'""")

    if not c.execute("SELECT 1 FROM projection_meta WHERE key='display_economics_version'").fetchone():
        from puppetmaster.job_display import touch
        for row in c.execute("SELECT id FROM jobs").fetchall():
            jid = row[0]
            stamp = c.execute("SELECT stamp FROM projection_current WHERE kind='job' AND job_id=?", (jid,)).fetchone()[0]
            touch(c, jid)
            c.execute("UPDATE projection_current SET stamp=? WHERE kind='job' AND job_id=?", (stamp, jid))
            c.execute("UPDATE projection_changes SET stamp=? WHERE revision=(SELECT revision FROM projection_current WHERE kind='job' AND job_id=?)", (stamp, jid))
            c.execute("UPDATE projection_current SET " + ','.join(
                field + '=(SELECT ' + expr + ' FROM jobs WHERE id=?)'
                for field, expr in zip(FIELDS, sql_fields('data')))
                + " WHERE kind='job' AND job_id=?", (jid,) * 5)
            member = c.execute("""SELECT
                CASE WHEN json_type(data,'$.cost_receipt.bounded_economics')='object'
                    AND length(CAST(json_extract(data,'$.cost_receipt.bounded_economics') AS BLOB))<=4096
                    THEN json_extract(data,'$.cost_receipt.bounded_economics') END,
                json_type(data,'$.cost_receipt')='object',json_extract(data,'$.status'),
                json_extract(data,'$.cost_receipt.job_id'),json_extract(data,'$.cost_receipt.pricing_source')
                FROM jobs WHERE id=?""", (jid,)).fetchone()
            from puppetmaster.selected_economics import project_file as project_economics_file
            receipt = ({'bounded_economics': metadata_json_loads(member[0])} if member[0] else {}) if member[1] else None
            if receipt is not None:
                receipt.update(job_id=member[3], pricing_source=member[4])
            project_economics_file(c, jid, {'status': member[2], 'cost_receipt': receipt})
        c.execute("INSERT INTO projection_meta VALUES('display_economics_version','1')")


def _reserve_writer(c, timeout=5.0):
    """Retry only lock acquisition, within one budget, before any effects."""
    from puppetmaster.readonly import _locked
    deadline = time.monotonic() + timeout
    try:
        while True:
            remaining = max(0.0, deadline - time.monotonic())
            c.execute(f"PRAGMA busy_timeout={int(min(.1, remaining) * 1000)}")
            try:
                c.execute("BEGIN IMMEDIATE")
                return
            except sqlite3.OperationalError as exc:
                if not _locked(exc) or time.monotonic() >= deadline:
                    raise
                time.sleep(min(.01, max(0.0, deadline - time.monotonic())))
    finally:
        c.execute("PRAGMA busy_timeout=5000")


@contextmanager
def connection(store, *, metadata_only=False, launch_binding=False, write=False, attach_deadline=None):
    if metadata_only:
        from puppetmaster.readonly import connect, selection
        from puppetmaster.identity import StoreIdentityError
        active = getattr(getattr(store, '_completion_connection', None), 'connection', None)
        if active is not None and selection(store) != store._read_selection:
            raise StoreIdentityError("store replaced during supervisor transaction")
        if active is not None:
            c = active
        elif attach_deadline is not None:
            c = connect(store, timeout=5, attach_binding=True, attach_deadline=attach_deadline)
        else:
            c = connect(store, timeout=5, reuse=True, launch_binding=launch_binding)
        c.row_factory = sqlite3.Row
        try:
            from puppetmaster.identity import read_identity, StoreIdentityError, legacy_schema
            legacy = legacy_schema(store, c)
            actual = None if legacy else read_identity(c, store.backend_name)
            if store._incarnation is not None and actual != store._incarnation:
                raise StoreIdentityError("store replaced since attach; explicitly reopen and rebind")
            store._incarnation = actual
            yield c
        finally:
            if active is None:
                c.close()
    elif store.backend_name == "sqlite":
        store._ensure_attached()
        with store._session() as c:
            yield c
    else:
        c = sqlite3.connect(str(store.root / "metadata.sqlite3"), timeout=5)
        c.row_factory = sqlite3.Row
        try:
            with c:
                if write:
                    # Reserve the writer before identity reads: a deferred
                    # read-to-write upgrade cannot wait behind another writer.
                    _reserve_writer(c)
                if store._incarnation is not None:
                    from puppetmaster.identity import read_identity, StoreIdentityError
                    if read_identity(c, "file") != store._incarnation:
                        raise StoreIdentityError("file metadata store replaced; explicitly reopen and rebind")
                yield c
        finally:
            c.close()


def initialize_file(store):
    with connection(store) as c:
        create_schema(c)
        from puppetmaster.identity import bootstrap
        legacy = not c.execute("SELECT 1 FROM projection_meta WHERE key='identity_version'").fetchone()
        store._incarnation = bootstrap(c, "file", legacy=legacy)
        c.execute("INSERT OR IGNORE INTO projection_meta VALUES('identity_version','2')")
        initialized = c.execute("SELECT 1 FROM projection_meta WHERE key='initialized'").fetchone()
        history_ready = c.execute("SELECT 1 FROM projection_meta WHERE key='history_version'").fetchone()
        completion_ready = c.execute("SELECT 1 FROM projection_meta WHERE key='completion_version'").fetchone()
        display_ready = c.execute("SELECT 1 FROM projection_meta WHERE key='display_economics_version'").fetchone()
        if initialized and history_ready and completion_ready and display_ready:
            return
        # Explicit supervisor migration only; readers never scan source files.
        for path in store.jobs_dir.glob("*/job.json"):
            directories = []
            if not initialized or not display_ready:
                project_file(c, path, store.read_json(path), legacy=True)
                if not initialized:
                    directories.extend(("tasks", "artifacts"))
            if not completion_ready:
                directories.append("completions")
            if not history_ready:
                directories.extend(("runs", "consumption/attempts", "consumption/observations"))
            for directory in directories:
                for child in (path.parent / directory).glob("*.json"):
                    project_file(c, child, store.read_json(child), legacy=True)
        c.execute("INSERT OR IGNORE INTO projection_meta VALUES('display_economics_version', '1')")
        c.execute("INSERT OR IGNORE INTO projection_meta VALUES('completion_version', '1')")
        c.execute("INSERT OR IGNORE INTO projection_meta VALUES('initialized', '1')")
        c.execute("INSERT OR IGNORE INTO projection_meta VALUES('history_version', '1')")


def project_file(c, path, value, legacy=False):
    kind = file_kind(path)
    if kind is None:
        return
    if kind == "completion":
        from puppetmaster.completion_metadata import project_file as project_completion
        project_completion(c, to_jsonable(value))
        return
    if kind in {"attempt", "run", "observation"}:
        from puppetmaster.history_metadata import project_file as project_history
        project_history(c, kind, value)
        return
    value = to_jsonable(value)
    jid = value["id"] if kind == "job" else value["job_id"]
    stamp = "legacy_unknown" if legacy else "known"
    args = (kind, jid, value["id"], value.get("execution_status" if kind == "artifact" else "status"), value.get("sha256"), stamp)
    scope = json.dumps({key: value.get(key) for key in ("origin", "project_id", "session_id")}) if kind == "job" else None
    cur = c.execute("""INSERT INTO projection_changes(kind,job_id,id,status,sha256,stamp,scope)
                       VALUES(?,?,?,?,?,?,?)""", (*args, scope))
    exists = c.execute("SELECT 1 FROM projection_current WHERE kind=? AND job_id=? AND id=?",
                       (kind, jid, value["id"])).fetchone()
    c.execute("""INSERT INTO projection_current
        (kind,job_id,id,status,sha256,revision,stamp,task_count,artifact_count,
         binding,task_id,artifact_type,scope) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(kind,job_id,id) DO UPDATE SET status=excluded.status,
        sha256=excluded.sha256, revision=excluded.revision, stamp=excluded.stamp,
        binding=excluded.binding, task_id=excluded.task_id, artifact_type=excluded.artifact_type,
        scope=excluded.scope""",
              (*args[:5], cur.lastrowid, stamp, 0 if kind == "job" else None, 0 if kind == "job" else None,
               json.dumps({"task_id": value["id"], "generation": value.get("generation"),
                           "lease_id": value.get("lease_id"), "owner": value.get("lease_owner")}) if kind == "task" else None,
               value.get("task_id") if kind == "artifact" else None,
               value.get("type") if kind == "artifact" else None, scope))
    if kind != "job":
        column = "task_count" if kind == "task" else "artifact_count"
        c.execute(f"UPDATE projection_current SET {column}=COALESCE({column},0)+? WHERE kind='job' AND job_id=?",
                  (0 if exists else 1, jid))
        cur = c.execute("""INSERT INTO projection_changes(kind,job_id,id,status,stamp,task_count,artifact_count)
            SELECT 'job',job_id,id,status,?,task_count,artifact_count
            FROM projection_current WHERE kind='job' AND job_id=?""", (stamp, jid))
        c.execute("UPDATE projection_current SET revision=?, stamp=? WHERE kind='job' AND job_id=?",
                  (cur.lastrowid, stamp, jid))
    else:
        from puppetmaster.job_display import project as project_display
        from puppetmaster.selected_economics import project_file as project_economics
        project_display(c, jid, value.get('goal'), value.get('status'))
        project_economics(c, jid, value)
        c.execute("""UPDATE projection_changes SET
            task_count=(SELECT task_count FROM projection_current WHERE kind='job' AND job_id=?),
            artifact_count=(SELECT artifact_count FROM projection_current WHERE kind='job' AND job_id=?)
            WHERE revision=?""", (jid, jid, cur.lastrowid))


def file_kind(path):
    if path.parent.name == "completions" and path.parent.parent.parent.name == "jobs":
        return "completion"
    if path.parent.name == "runs" and path.parent.parent.parent.name == "jobs":
        return "run"
    if path.parent.parent.name == "consumption" and path.parent.parent.parent.parent.name == "jobs":
        return {"attempts": "attempt", "observations": "observation"}.get(path.parent.name)
    if path.name == "job.json" and path.parent.parent.name == "jobs":
        return "job"
    if path.parent.name in {"tasks", "artifacts"} and path.parent.parent.parent.name == "jobs":
        return "task" if path.parent.name == "tasks" else "artifact"
    return None


def page(store, kind, job_ref=None, *, cursor=None, limit=100, max_bytes=MAX_BYTES,
         max_scan=MAX_SCAN, changes=False, after_revision=0, status=None,
         origin=None, project_id=None, session_id=None):
    if type(limit) is not int or not 1 <= limit <= MAX_PAGE:
        raise ValueError("limit must be between 1 and 200")
    if type(max_bytes) is not int or not 1024 <= max_bytes <= MAX_BYTES:
        raise ValueError("max_bytes must be between 1024 and 262144")
    if type(max_scan) is not int or not 1 <= max_scan <= MAX_SCAN:
        raise ValueError("max_scan must be between 1 and 1000")
    if type(after_revision) is not int or not 0 <= after_revision <= 9223372036854775807:
        raise ValueError("invalid revision")
    if job_ref is not None:
        if not isinstance(job_ref, JobRef) or job_ref.state_id != state_identity(store.root):
            raise ValueError("job_ref.state_id does not match this store")
        store._assert_safe_job_dir(job_ref.job_id)
    if status is not None and (not isinstance(status, str) or len(status) > 64):
        raise ValueError("invalid status filter")
    filters = {"origin": origin, "project_id": project_id, "session_id": session_id}
    for name, value in filters.items():
        if value is not None and (kind != "job" or not isinstance(value, str) or not value or len(value) > 256):
            raise ValueError(f"invalid {name} filter")
    if cursor is not None:
        CursorCodec.inspect(cursor)
    sid = state_identity(store.root)
    scope = immutable_digest([sid, kind, to_jsonable(job_ref), changes, after_revision, status, filters])
    try:
        database = store.root / ("state.sqlite3" if store.backend_name == "sqlite" else "metadata.sqlite3")
        if not database.exists():
            return MetadataPage((), "unavailable", after_revision if changes else 0, next_cursor=cursor)
        with connection(store, metadata_only=True) as c:
            if not c.in_transaction:
                c.execute("BEGIN")
            if c.execute("SELECT 1 FROM projection_pending LIMIT 1").fetchone():
                return MetadataPage((), "unavailable", after_revision if changes else 0, next_cursor=cursor)
            from puppetmaster.identity import scope_identity, validate, make_ref
            incarnation = scope_identity(store, job_ref, c)
            if job_ref:
                validate(store, job_ref, c, strict=changes and kind == "job")
            scope = immutable_digest([scope, incarnation])
            rev = c.execute("SELECT COALESCE(MAX(revision),0) FROM projection_changes").fetchone()[0]
            from puppetmaster.metadata_snapshot import meta_scalar
            secret = meta_scalar(c, "secret")
            codec = CursorCodec(bytes.fromhex(secret))
            epoch = meta_scalar(c, "epoch")
            value = codec.decode(cursor, scope) if cursor else {}
            if cursor:
                if type(value.get('revision')) is not int or not 0 <= value['revision'] <= 9223372036854775807:
                    raise ValueError('invalid metadata cursor')
                keys = [value.get('boundary')] if changes else [value.get('boundary'), value.get('last')]
                if changes and (type(value.get('last')) is not int or not 0 <= value['last'] <= value['revision']):
                    raise ValueError('invalid metadata cursor')
                if any(key is not None and (not isinstance(key, list) or len(key) != 2
                       or any(not isinstance(part, str) for part in key)) for key in keys):
                    raise ValueError('invalid metadata cursor')
            if value.get("epoch", epoch) != epoch:
                return MetadataPage((), "cursor_expired", after_revision if changes else rev, next_cursor=cursor)
            if job_ref and not changes and not c.execute("SELECT 1 FROM projection_current WHERE kind='job' AND job_id=? LIMIT 1",
                                         (job_ref.job_id,)).fetchone():
                return MetadataPage((), "unavailable", after_revision if changes else 0, next_cursor=cursor)
            floor = meta_scalar(c, "retention_floor") if changes else None
            if changes and after_revision < int(floor):
                return MetadataPage((), "unavailable", after_revision, next_cursor=cursor,
                                    reason="change_history_unavailable")
            snapshot = value.get("revision", rev)
            if after_revision > rev or snapshot > rev:
                return MetadataPage((), "cursor_expired", after_revision if changes else rev, next_cursor=cursor)
            last = value.get("last", after_revision if changes else ["", ""])
            args = [kind]
            where = "kind=?"
            if status is not None and not changes:
                where += " AND status=?"
                args.append(status)
            if job_ref:
                where += " AND job_id=?"
                args.append(job_ref.job_id)
            table = "projection_changes" if changes else "projection_current"
            key = "revision" if changes else "id"
            where += f" AND {key}>?"
            args.append(last if changes else last[0])
            if changes:
                where += " AND revision<=?"
                args.append(snapshot)
            query = f"SELECT * FROM {table} WHERE {where}"
            from puppetmaster.metadata_snapshot import bounded_columns, high_key, rows as snapshot_rows, SCALAR_BYTES
            scalar_limit = min(SCALAR_BYTES, max_bytes)
            display_supported = bool(c.execute("SELECT 1 FROM projection_meta WHERE key='display_economics_version' AND value='1'").fetchone())
            boundary = value.get("boundary")
            if not changes and boundary is None:
                boundary = high_key(c, kind, job_ref, scalar_limit)
                if boundary is None:
                    return MetadataPage((), "unavailable", 0, next_cursor=cursor)
            if changes:
                from puppetmaster.metadata_snapshot import COLUMNS
                columns = COLUMNS + ("deleted", "previous_status", "previous_scope", "previous_membership")
                query = query.replace("SELECT *", "SELECT " + bounded_columns(columns, max_bytes=scalar_limit, integer_columns=("revision",), display_supported=display_supported))
                rows = c.execute(query + f" ORDER BY {key} LIMIT ?",
                                 (*args, min(limit + 1, max_scan))).fetchall()
            else:
                rows, scanned = snapshot_rows(c, kind, job_ref, snapshot, last, boundary,
                                              min(limit + 1, max_scan), scalar_limit, display_supported)
                if rows is None:
                    return MetadataPage((), "unavailable", 0, next_cursor=cursor, scanned=scanned)
            items = []
            consumed = 0
            for row in rows:
                if len(items) == limit:
                    break
                if row["oversized"]:
                    return MetadataPage((), "unavailable", after_revision if changes else 0, next_cursor=cursor, scanned=len(rows))
                row_key = row[key] if changes else [row['id'], row['job_id']]
                if not changes and row['deleted']:
                    last = row_key
                    consumed += 1
                    continue
                if changes and kind == "job" and (row["previous_membership"] not in ("present", "absent")
                        or (row["previous_membership"] == "present" and row["previous_scope"] is None)):
                    return MetadataPage((), "unavailable", after_revision, next_cursor=cursor,
                                        scanned=len(rows), reason="previous_membership_unavailable")
                try:
                    current_scope = metadata_json_loads(row["scope"] or "{}")
                    previous_scope = metadata_json_loads(row["previous_scope"] or "{}") if changes else {}
                    if not isinstance(current_scope, dict) or not isinstance(previous_scope, dict):
                        raise ValueError("invalid scope")
                    if any(v is not None and not isinstance(v, str)
                           for scope_row in (current_scope, previous_scope) for v in scope_row.values()):
                        raise ValueError("invalid scope fields")
                except (ValueError, TypeError):
                    return MetadataPage((), "unavailable", after_revision if changes else 0,
                                        next_cursor=cursor, scanned=len(rows), reason="membership_invalid")
                current_matches = (status is None or row["status"] == status) and all(
                    value is None or current_scope.get(name) == value for name, value in filters.items())
                previous_matches = changes and row["previous_membership"] == "present" and (status is None or row["previous_status"] == status) and all(
                    value is None or previous_scope.get(name) == value for name, value in filters.items())
                if not current_matches and not previous_matches:
                    last = row_key
                    consumed += 1
                    continue
                try:
                    if row['delivery'] not in ('pending', 'blocked', 'unverified', 'unavailable') or row['quality'] not in ('unverified', 'unavailable'):
                        raise ValueError('invalid display state')
                    if (row['goal_preview'] is None) != (row['goal_preview_truncated'] is None):
                        raise ValueError('invalid preview pairing')
                    item = MetadataRef(job_ref if job_ref is not None and job_ref.version == 1 else make_ref(store.root, row["job_id"], incarnation), row["id"], kind,
                        row["status"], row["sha256"], row["revision"], row["stamp"],
                        (bool(row["deleted"]) or not current_matches) if changes else False,
                        row["task_count"], row["artifact_count"],
                        TaskBinding(**metadata_json_loads(row["binding"])) if row["binding"] else None,
                        row["task_id"], row["artifact_type"],
                        current_scope.get("origin"), current_scope.get("project_id"), current_scope.get("session_id"),
                        row["previous_membership"] if changes else "unavailable",
                        row["previous_status"] if changes else None,
                        previous_scope.get("origin"), previous_scope.get("project_id"), previous_scope.get("session_id"),
                        row["goal_preview"], None if row["goal_preview_truncated"] is None else bool(row["goal_preview_truncated"]),
                        row["delivery"], row["quality"])
                except (ValueError, TypeError, KeyError):
                    return MetadataPage((), "unavailable", after_revision if changes else 0,
                                        next_cursor=cursor, scanned=len(rows), reason="metadata_invalid")
                candidate_token = codec.encode({"v": 1, "scope": scope, "revision": snapshot, "last": row_key, "epoch": epoch, "boundary": boundary})
                candidate = MetadataPage(tuple(items + [item]), "partial", snapshot, candidate_token, len(rows))
                if len(json.dumps(to_jsonable(candidate), ensure_ascii=True).encode()) > max_bytes:
                    break
                items.append(item)
                consumed += 1
                last = row_key
            if rows and not consumed:
                return MetadataPage((), "unavailable", after_revision if changes else 0, next_cursor=cursor, scanned=len(rows))
            more = consumed < len(rows) or len(rows) == min(limit + 1, max_scan)
            token = codec.encode({"v": 1, "scope": scope, "revision": snapshot, "last": last, "epoch": epoch, "boundary": boundary}) if more else None
            return MetadataPage(tuple(items), "partial" if more else "complete", snapshot, token, len(rows))
    except sqlite3.OperationalError as exc:
        from puppetmaster.readonly import ReadUnavailable
        if isinstance(exc, ReadUnavailable):
            return MetadataPage((), "unavailable", after_revision if changes else 0,
                                next_cursor=cursor, reason="read_snapshot_unavailable",
                                retry_after_ms=exc.retry_after_ms)
        code = getattr(exc, "sqlite_errorcode", None)
        # Python 3.9 has no sqlite_errorcode; match SQLite's lock messages only.
        locked = ((code & 0xff) in (5, 6)) if code is not None else str(exc) in {
            "database is locked", "database table is locked", "database schema is locked",
        } or str(exc).startswith(("database table is locked: ", "database schema is locked: "))
        if locked:
            return MetadataPage((), "unavailable", after_revision if changes else 0,
                                next_cursor=cursor, reason="read_snapshot_unavailable", retry_after_ms=100)
        if any(message in str(exc) for message in ("no such table", "unable to open database")):
            return MetadataPage((), "unavailable", after_revision, next_cursor=cursor)
        raise
