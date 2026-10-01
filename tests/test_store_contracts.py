"""Hermetic public contract parity, replay, cursor and crash regression tests."""
import json
import sqlite3
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import sys
sys.path.insert(0, str(Path(__file__).parent))
import hermetic_env  # noqa: F401

from puppetmaster.attempts import ExecutionAttempt
from puppetmaster.contracts import ContractConflict, EffectReceipt, immutable_digest
from puppetmaster.models import AgentRun, Artifact, ArtifactType, JobRef, Task, TaskStatus, now_iso, to_jsonable
from puppetmaster.sqlite_store import SQLiteSwarmStore
from puppetmaster.state import resolve_job_state, state_identity
from puppetmaster.store import SwarmStore
from puppetmaster.store_contracts import task_binding


class StoreContractTests(unittest.TestCase):
    def test_migration_and_metadata_connections_close_without_gc(self):
        connect = sqlite3.connect
        for name in (
            "test_metadata_read_does_not_run_migration",
            "test_projection_schema_change_drops_invalid_source_triggers_first",
            "test_sqlite_v4_migration_marks_legacy_unknown",
            "test_v5_init_repairs_old_cardinality_triggers",
        ):
            with self.subTest(test=name):
                connections = []

                def tracked_connect(*args, **kwargs):
                    c = connect(*args, **kwargs)
                    connections.append(c)
                    return c

                try:
                    # Retain handles so GC cannot hide Windows file-lock leaks.
                    with patch("sqlite3.connect", side_effect=tracked_connect):
                        getattr(self, name)()
                    self.assertTrue(connections)
                    for c in connections:
                        with self.assertRaises(sqlite3.ProgrammingError):
                            c.execute("SELECT 1")
                finally:
                    for c in connections:
                        c.close()

    def test_public_store_import_uses_supported_python_syntax(self):
        import ast
        import subprocess
        import puppetmaster.projections as projections
        ast.parse(Path(projections.__file__).read_text(), feature_version=(3, 9))
        result = subprocess.run([sys.executable, "-c",
            "from puppetmaster.store import SwarmStore; "
            "from puppetmaster.sqlite_store import SQLiteSwarmStore; "
            "import puppetmaster.projections"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_filtered_membership_transitions_and_ref_deletion(self):
        from puppetmaster.models import JobStatus
        for store, job, task, run, ref in self.stores():
            for scoped in (False, True):
                filters = {"job_ref": ref} if scoped else {}
                store.save_job(replace(job, status=JobStatus.RUNNING))
                snapshot = store.list_job_summaries(status="running", **filters)
                self.assertEqual([i.id for i in snapshot.items], [job.id])
                store.save_job(replace(job, status=JobStatus.COMPLETE))
                store.save_job(replace(job, status=JobStatus.RUNNING))
                store.save_job(replace(job, status=JobStatus.COMPLETE))
                with patch.object(store, "get_job", side_effect=AssertionError("body")), \
                     patch.object(store, "read_json", side_effect=AssertionError("body")):
                    page = store.read_job_summary_changes(after_revision=snapshot.revision,
                        status="running", limit=1, max_scan=2, max_bytes=1500, **filters)
                    items = list(page.items)
                    self.assertTrue(page.next_cursor)
                    with self.assertRaises(ValueError):
                        store.read_job_summary_changes(after_revision=snapshot.revision,
                            status="complete", cursor=page.next_cursor, **filters)
                    while page.next_cursor:
                        page = store.read_job_summary_changes(after_revision=snapshot.revision,
                            status="running", cursor=page.next_cursor, limit=1, max_scan=2,
                            max_bytes=1500, **filters)
                        self.assertLessEqual(page.scanned, 2)
                        self.assertLessEqual(len(json.dumps(to_jsonable(page)).encode()), 1500)
                        items.extend(page.items)
                self.assertEqual([i.deleted for i in items], [True, False, True])
                self.assertEqual([i.revision for i in items], sorted(set(i.revision for i in items)))
                self.assertFalse(store.read_job_summary_changes(after_revision=snapshot.revision,
                    status="failed", **filters).items)
            before = store.list_job_summaries(job_ref=ref).revision
            store.delete_job(job.id)
            for filters in ({}, {"status": "complete"}, {"job_ref": ref},
                            {"status": "complete", "job_ref": ref}):
                page = store.read_job_summary_changes(after_revision=before, **filters)
                self.assertEqual(page.outcome, "complete")
                self.assertTrue(any(i.id == job.id and i.deleted for i in page.items))

    def test_scope_filters_transitions_tokens_and_deletion(self):
        from itertools import combinations
        from puppetmaster.contracts import JobSummaryFilter
        from puppetmaster.models import JobStatus
        fields = {"origin": "host", "project_id": "project", "session_id": "session"}
        for store, legacy, task, run, legacy_ref in self.stores():
            job = store.create_job("private goal", **fields)
            ref = store.job_ref(job.id)
            for size in (1, 2, 3):
                for names in combinations(fields, size):
                    filters = {name: fields[name] for name in names}
                    for extra in ({}, {"status": "queued"}, {"job_ref": ref},
                                  {"status": "queued", "job_ref": ref}):
                        result = store.list_job_summaries(JobSummaryFilter(**filters, **extra))
                        self.assertEqual([i.id for i in result.items], [job.id])
                        self.assertEqual(result.items[0].origin, "host")
                        self.assertEqual(result.items[0].project_id, "project")
                        self.assertEqual(result.items[0].session_id, "session")
            for name in fields:
                self.assertFalse(store.list_job_summaries(**{name: fields[name].upper()}).items)
                filters = {name: fields[name], "status": "queued", "job_ref": ref}
                before = store.list_job_summaries(**filters).revision
                store.save_job(replace(job, **{name: "elsewhere"}))
                store.save_job(job)
                store.save_job(replace(job, **{name: None}))
                page = store.read_job_summary_changes(after_revision=before, limit=1, **filters)
                self.assertTrue(page.next_cursor)
                with self.assertRaises(ValueError):
                    store.read_job_summary_changes(after_revision=before, cursor=page.next_cursor,
                                                   **{**filters, name: "elsewhere"})
                items = list(page.items)
                while page.next_cursor:
                    page = store.read_job_summary_changes(after_revision=before,
                        cursor=page.next_cursor, limit=1, max_scan=1, **filters)
                    items.extend(page.items)
                self.assertEqual([i.deleted for i in items], [True, False, True])
                store.save_job(job)
            # Neither old nor new combined membership matches: don't combine
            # the old scope with the new status and invent a removal/upsert.
            store.save_job(replace(job, status=JobStatus.RUNNING))
            before = store.list_job_summaries().revision
            store.save_job(replace(job, origin="elsewhere"))
            self.assertFalse(store.read_job_summary_changes(after_revision=before,
                origin="host", status="queued").items)
            store.save_job(job)
            before = store.list_job_summaries().revision
            store.save_task(replace(task, job_id=job.id, id="scope_task"))
            count_change = store.read_job_summary_changes(after_revision=before, **fields)
            self.assertEqual(len(count_change.items), 1)
            self.assertEqual(count_change.items[0].task_count, 1)
            before = count_change.revision
            store.delete_job(job.id)
            for filters in (fields, {**fields, "job_ref": ref, "status": "queued"}):
                result = store.read_job_summary_changes(after_revision=before, **filters)
                self.assertTrue(any(i.deleted and i.id == job.id for i in result.items))
            self.assertIsNone(store.get_job(legacy.id).origin)

    def test_scope_scan_is_bounded_and_never_hydrates(self):
        from contextlib import contextmanager
        from puppetmaster.projections import connection
        for store, job, task, run, ref in self.stores():
            with connection(store) as c:
                c.executemany("""INSERT INTO projection_changes(kind,job_id,id,status,stamp)
                    VALUES('job',?,?,'queued','known')""",
                    [("unrelated_" + str(i), "unrelated_" + str(i)) for i in range(4000)])
            steps = []
            @contextmanager
            def measured(*args, **kwargs):
                with connection(*args, **kwargs) as c:
                    c.set_progress_handler(lambda: steps.append(1) or 0, 100)
                    yield c
            with patch("puppetmaster.projections.connection", measured), \
                 patch.object(store, "get_job", side_effect=AssertionError("body")), \
                 patch.object(store, "read_json", side_effect=AssertionError("body")):
                page = store.read_job_summary_changes(origin="host", max_scan=2)
                self.assertEqual(page.outcome, "partial")
                self.assertEqual(page.scanned, 2)
                self.assertFalse(page.items)
                self.assertTrue(page.next_cursor)
            self.assertLess(len(steps), 30)
            # Sparse snapshots advance through empty pages and bind every field.
            store.create_job("match", origin="host", project_id="p", session_id="s")
            for name, value in (("origin", "host"), ("project_id", "p"), ("session_id", "s")):
                page = store.list_job_summaries(max_scan=1, **{name: value})
                self.assertTrue(page.next_cursor)
                with self.assertRaises(ValueError):
                    store.list_job_summaries(cursor=page.next_cursor, **{name: "other"})
                items = list(page.items)
                while page.next_cursor:
                    page = store.list_job_summaries(cursor=page.next_cursor, max_scan=1, **{name: value})
                    items.extend(page.items)
                self.assertEqual(len(items), 1)

    def test_scope_serialization_launch_identity_and_reopen(self):
        from puppetmaster.models import Job, job_from_dict
        from puppetmaster.store import LaunchConflictError
        old = to_jsonable(Job(goal="legacy"))
        for name in ("origin", "project_id", "session_id"):
            old.pop(name)
        self.assertEqual(job_from_dict(old).origin, None)
        for store, job, task, run, ref in self.stores():
            scoped, created = store.create_or_get_job("scope", launch_key="key",
                origin="host", project_id="p", session_id="s")
            self.assertTrue(created)
            self.assertEqual(job_from_dict(to_jsonable(scoped)), scoped)
            reopened = type(store)(store.root)
            reopened.init()
            self.assertEqual(reopened.get_job(scoped.id), scoped)
            self.assertEqual(reopened.list_job_summaries(origin="host").items[0].id, scoped.id)
            self.assertFalse(reopened.create_or_get_job("scope", launch_key="key",
                origin="host", project_id="p", session_id="s")[1])
            for name in ("origin", "project_id", "session_id"):
                fields = dict(origin="host", project_id="p", session_id="s")
                fields[name] = "changed"
                with self.assertRaises(LaunchConflictError):
                    reopened.create_or_get_job("scope", launch_key="key", **fields)
                with self.assertRaises(ValueError):
                    reopened.create_job("invalid", **{name: 123})

    def test_scope_schema_upgrade_keeps_legacy_membership_unknown(self):
        from puppetmaster.projections import connection
        for store, job, task, run, ref in self.stores():
            before = store.list_job_summaries().revision
            with connection(store) as c:
                c.execute("DROP TRIGGER projection_scope")
                for table in ("jobs", "tasks", "artifacts"):
                    for operation in ("INSERT", "UPDATE", "DELETE"):
                        c.execute(f"DROP TRIGGER IF EXISTS projection_{table}_{operation}")
                for operation in ("INSERT", "UPDATE", "DELETE"):
                    c.execute(f"DROP TRIGGER IF EXISTS projection_version_{operation}")
                c.execute("ALTER TABLE projection_current DROP COLUMN scope")
                c.execute("ALTER TABLE projection_changes DROP COLUMN scope")
                c.execute("ALTER TABLE projection_changes DROP COLUMN previous_scope")
            reopened = type(store)(store.root)
            reopened.init()
            self.assertEqual(reopened.list_job_summaries().revision, before)
            self.assertFalse(reopened.list_job_summaries(origin="host").items)
            self.assertFalse(reopened.read_job_summary_changes(origin="host").items)
            reopened.save_job(replace(job, origin="host"))
            changes = reopened.read_job_summary_changes(after_revision=before, origin="host")
            if store.backend_name == "file":
                self.assertEqual(changes.outcome, "unavailable")
                self.assertEqual(changes.reason, "previous_membership_unavailable")
                self.assertEqual(changes.revision, before)
                self.assertIsNone(changes.next_cursor)
            else:
                # Supervisor recovered current source ownership before this new
                # write; earlier journal authority remains unavailable.
                self.assertEqual([(i.id, i.deleted) for i in changes.items], [(job.id, False)])
                self.assertEqual(changes.items[0].previous_membership, "present")
                self.assertIsNone(changes.items[0].previous_origin)

    def test_v5_init_repairs_old_cardinality_triggers(self):
        from puppetmaster.models import JobStatus
        with TemporaryDirectory() as tmp:
            store = SQLiteSwarmStore(Path(tmp))
            store.init()
            job = store.create_job("existing", origin="host", project_id="p", session_id="s")
            store.save_job(replace(job, status=JobStatus.RUNNING))
            with closing(sqlite3.connect(store.db_path)) as c, c:
                history = c.execute("SELECT * FROM projection_changes ORDER BY revision").fetchall()
                current = c.execute("SELECT * FROM projection_current ORDER BY kind,id").fetchall()
                self.assertEqual(len(c.execute("PRAGMA table_info(projection_current)").fetchall()), 17)
                self.assertEqual(c.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()[0], "7")
                for operation in ("INSERT", "UPDATE"):
                    c.execute(f"DROP TRIGGER projection_jobs_{operation}")
                    c.execute(f"""CREATE TRIGGER projection_jobs_{operation}
                        AFTER {operation} ON jobs BEGIN
                        INSERT INTO projection_current VALUES(
                            'job',NEW.id,NEW.id,json_extract(NEW.data,'$.status'),NULL,
                            0,'known',0,0,NULL,NULL,NULL);
                        END""")
            for _ in range(2):
                store = SQLiteSwarmStore(Path(tmp))
                store.init()
                with closing(sqlite3.connect(store.db_path)) as c, c:
                    self.assertEqual(c.execute("SELECT * FROM projection_changes ORDER BY revision").fetchall(), history)
                    self.assertEqual(c.execute("SELECT * FROM projection_current ORDER BY kind,id").fetchall(), current)
            fresh, created = store.create_or_get_job("new", launch_key="repair",
                origin="host", project_id="p", session_id="s")
            self.assertTrue(created)
            store.save_job(replace(fresh, status=JobStatus.RUNNING, origin="other"))
            store.save_job(replace(job, status=JobStatus.COMPLETE, origin="other"))
            with closing(sqlite3.connect(store.db_path)) as c, c:
                row = c.execute("SELECT status,previous_status,scope,previous_scope FROM projection_changes "
                                "WHERE id=? ORDER BY revision DESC LIMIT 1", (job.id,)).fetchone()
                self.assertEqual(row[:2], ("complete", "running"))
                self.assertEqual(json.loads(row[2])["origin"], "other")
                self.assertEqual(json.loads(row[3])["origin"], "host")

    def test_projection_schema_change_drops_invalid_source_triggers_first(self):
        with TemporaryDirectory() as tmp:
            store = SQLiteSwarmStore(Path(tmp))
            store.init()
            with closing(sqlite3.connect(store.db_path)) as c, c:
                c.execute("DROP TRIGGER projection_scope")
                c.execute("ALTER TABLE projection_changes DROP COLUMN previous_scope")
                c.execute("DROP TRIGGER projection_jobs_INSERT")
                c.execute("""CREATE TRIGGER projection_jobs_INSERT AFTER INSERT ON jobs BEGIN
                    INSERT INTO projection_current VALUES(
                        'job',NEW.id,NEW.id,NULL,NULL,0,'known',0,0,NULL,NULL,NULL);
                    END""")
            reopened = SQLiteSwarmStore(Path(tmp))
            reopened.init()
            job = reopened.create_job("repaired", origin="host")
            self.assertEqual(reopened.list_job_summaries(origin="host").items[0].id, job.id)

    def test_projection_writes_allow_additional_columns(self):
        from puppetmaster.projections import connection
        for store, job, task, run, ref in self.stores():
            with connection(store) as c:
                c.execute("ALTER TABLE projection_current ADD COLUMN future_metadata TEXT")
                c.execute("ALTER TABLE projection_changes ADD COLUMN future_metadata TEXT")
            reopened = type(store)(store.root)
            reopened.init()
            fresh = reopened.create_job("new", origin="host")
            reopened.save_job(replace(fresh, origin="other"))
            reopened.save_task(replace(task, id="future_task", job_id=fresh.id))
            self.assertEqual(reopened.list_job_summaries(origin="other").items[0].id, fresh.id)

    def test_projection_upgrade_preserves_status_history(self):
        from puppetmaster.projections import connection
        from puppetmaster.models import JobStatus
        for store, job, task, run, ref in self.stores():
            store.save_job(replace(job, status=JobStatus.RUNNING))
            revision = store.list_job_summaries().revision
            store.save_job(replace(job, status=JobStatus.COMPLETE))
            with connection(store) as c:
                c.execute("DROP TRIGGER projection_previous_status")
                c.execute("DROP INDEX projection_previous_filter")
                c.execute("DROP INDEX projection_previous_scoped_filter")
                # Reconstruct the pre-repair journal schema, retaining revisions.
                columns = [r for r in c.execute("PRAGMA table_info(projection_changes)")
                           if r[1] != "previous_status"]
                definitions = [r[1] + " " + r[2] + (" PRIMARY KEY AUTOINCREMENT" if r[5] else "")
                               for r in columns]
                names = ",".join(r[1] for r in columns)
                c.execute("CREATE TABLE old_changes(" + ",".join(definitions) + ")")
                c.execute("INSERT INTO old_changes SELECT " + names + " FROM projection_changes")
                triggers = c.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger'").fetchall()
                for name, sql in triggers:
                    c.execute('DROP TRIGGER "' + name + '"')
                c.execute("DROP TABLE projection_changes")
                c.execute("ALTER TABLE old_changes RENAME TO projection_changes")
                for name, sql in triggers:
                    c.execute(sql)
            reopened = type(store)(store.root)
            reopened.init()
            items = reopened.read_job_summary_changes(after_revision=revision, status="running").items
            self.assertEqual([(i.id, i.deleted) for i in items], [(job.id, True)])

    def test_filtered_feed_scan_cost_is_bounded(self):
        from contextlib import contextmanager
        from puppetmaster.projections import connection
        for store, job, task, run, ref in self.stores():
            with connection(store) as c:
                c.executemany("""INSERT INTO projection_changes(kind,job_id,id,status,stamp)
                    VALUES('job',?,?,'running','known')""",
                    [("unrelated_" + str(i), "unrelated_" + str(i)) for i in range(4000)])
            steps = []
            @contextmanager
            def measured(*args, **kwargs):
                with connection(*args, **kwargs) as c:
                    c.set_progress_handler(lambda: steps.append(1) or 0, 100)
                    yield c
            with patch("puppetmaster.projections.connection", measured):
                page = store.read_job_summary_changes(status="running", job_ref=ref,
                    max_scan=1, limit=1)
            self.assertFalse(page.items)
            self.assertLess(len(steps), 30, "scanned unrelated journal history")
            steps.clear()
            with patch("puppetmaster.projections.connection", measured):
                page = store.read_job_summary_changes(status="failed", max_scan=1, limit=1)
            self.assertFalse(page.items)
            self.assertLess(len(steps), 30, "scanned unrelated statuses")

    def stores(self):
        for backend in (SwarmStore, SQLiteSwarmStore):
            with TemporaryDirectory() as tmp:
                store = backend(Path(tmp))
                job = store.create_job("contracts")
                task = Task(job_id=job.id, role="explore", instruction="private large instruction")
                store.save_task(task)
                task = store.claim_task(task.id, "worker")
                run = AgentRun(job_id=job.id, task_id=task.id, role=task.role,
                               worker_id="worker", status=TaskStatus.COMPLETE, completed_at=now_iso())
                yield store, job, task, run, store.job_ref(job.id)

    def test_completion_retry_conflict_reopen(self):
        for store, job, task, run, ref in self.stores():
            event = {"task_id": task.id}
            with patch.object(store, "reconcile_completions"):
                store.complete_task(task, run, [], event)
                self.assertEqual(store.get_completion_receipt(ref, run.id).outcome, "pending_publication")
                with self.assertRaises(ContractConflict):
                    store.complete_task(task, run, [], {"task_id": "changed"})
            reopened = type(store)(store.root)
            receipt = reopened.submit_completion(task, run, [], event, job_ref=ref)
            self.assertEqual(receipt.outcome, "published")
            self.assertEqual(reopened.submit_completion(task, run, [], event, job_ref=ref), receipt)
            self.assertEqual(sum(e['event'] == 'worker.completed_task' for e in reopened.read_events(job.id)), 1)
            with self.assertRaises(ContractConflict):
                reopened.complete_task(task, run, [], {"extra": True})

    def test_completion_invalidation_and_legacy(self):
        for store, job, task, run, ref in self.stores():
            with patch.object(store, "reconcile_completions"):
                store.complete_task(task, run, [], {})
            store.save_task(replace(task, lease_id="successor"))
            store.reconcile_completions(job.id)
            self.assertEqual(store.get_completion_receipt(ref, run.id).outcome, "invalidated")
            stale = store.submit_completion(task, replace(run, id="run_stale"), [], {}, job_ref=ref)
            self.assertEqual(stale.outcome, "stale_lease")
            self.assertEqual(store.get_completion_receipt(ref, "missing").outcome, "legacy_unknown")

    def test_metadata_bounds_opaque_tokens_no_hydration(self):
        for store, job, task, run, ref in self.stores():
            for index in range(4):
                store.save_task(Task(job_id=job.id, role="explore", instruction="x" * 100000, id=f"task_{index}"))
                store.save_artifact(Artifact(job_id=job.id, task_id=task.id, type=ArtifactType.FINDING,
                                           payload={"claim": "x" * 100000}, created_by="worker", confidence=1, evidence=["test:metadata"]))
            with patch.object(store, "get_task_by_id", side_effect=AssertionError("body")), \
                 patch.object(store, "get_job", side_effect=AssertionError("body")), \
                 patch.object(store, "list_artifacts", side_effect=AssertionError("body")), \
                 patch.object(store, "read_json", side_effect=AssertionError("body")):
                page = store.list_task_refs(ref, limit=2)
                self.assertEqual(len(page.items), 2)
                self.assertEqual(page.outcome, "partial")
                next_page = store.list_task_refs(ref, cursor=page.next_cursor, limit=2)
                self.assertFalse({i.id for i in page.items} & {i.id for i in next_page.items})
                self.assertNotIn("instruction", json.dumps(to_jsonable(page)))
                self.assertEqual(len(store.list_artifact_refs(ref).items), 4)
                with self.assertRaises(ValueError):
                    store.list_artifact_refs(ref, cursor=page.next_cursor)
                with self.assertRaises(ValueError):
                    store.list_task_refs(ref, cursor=page.next_cursor[:-3] + "AAA")
            for bounds in ({"limit": 201}, {"max_scan": 1001}, {"max_bytes": 262145}, {"limit": True}):
                with self.assertRaises(ValueError):
                    store.list_job_summaries(**bounds)
            store.save_task(replace(task, status=TaskStatus.FAILED))
            self.assertNotEqual(store.list_task_refs(ref, cursor=page.next_cursor).outcome, "cursor_expired")

    def test_deletion_tombstones_and_change_revision(self):
        for store, job, task, run, ref in self.stores():
            before = store.list_job_summaries().revision
            store.delete_job(job.id)
            changes = store.read_job_summary_changes(after_revision=before)
            self.assertTrue(any(i.deleted and i.id == job.id for i in changes.items))
            self.assertFalse(store.list_job_summaries().items)

    def test_cancellation_reopen_conflict_and_stale_binding(self):
        for store, job, task, run, ref in self.stores():
            binding = task_binding(task)
            receipt = store.request_cancellation(ref, "cancel_1", [binding])
            self.assertEqual(receipt.outcome, "requested")
            reopened = type(store)(store.root)
            self.assertTrue(reopened.cancellation_pending(ref, binding))
            self.assertEqual(reopened.request_cancellation(ref, "cancel_1", [binding]), receipt)
            self.assertEqual(reopened.request_cancellation(ref, "cancel_1", [replace(binding, generation=99)]).outcome, "conflict")
            successor = replace(task, lease_id="next_lease", attempts=task.attempts + 1)
            reopened.save_task(successor)
            self.assertFalse(reopened.cancellation_pending(ref, task_binding(successor)))
            self.assertEqual(reopened.request_cancellation(ref, "cancel_stale", [binding]).outcome, "stale_binding")
            reopened.observe_cancellation(ref, binding)
            self.assertEqual(reopened.get_cancellation_receipt(ref, "cancel_1").outcome, "observed_stop")

    def test_contract_writes_survive_a_source_move_during_their_precheck(self):
        # Windows CI: a worker's cancellation observe failed with "source
        # changed" because a concurrent writer moved the source while the
        # readonly identity precheck opened. Identity reads retry contention.
        from puppetmaster import readonly
        real_connect = readonly.connect
        for store, job, task, run, ref in self.stores():
            binding = task_binding(task)
            attempt = ExecutionAttempt.from_run(run, adapter="local")
            store.record_attempt(attempt)
            intent = EffectReceipt(ref, "effect_rw", immutable_digest({"command": "rw"}),
                                   binding, run.id, attempt.attempt_id, 1,
                                   "not_dispatched", "reconcile_first")
            store.record_effect(intent)
            moves = []

            def moved_once(*args, **kwargs):
                if len(moves) < 3:
                    moves.append(1)
                    raise readonly.ReadUnavailable("unable to open database: source changed")
                return real_connect(*args, **kwargs)
            with patch("puppetmaster.readonly.connect", side_effect=moved_once):
                self.assertEqual(store.advance_effect(ref, intent.effect_id, expected_revision=1,
                                                      outcome="in_flight", evidence_refs=("d",)).revision, 2)
                moves.clear()
                self.assertEqual(store.request_cancellation(ref, "cancel_rw", [binding]).outcome, "requested")
                moves.clear()
                store.observe_cancellation(ref, binding)
            self.assertEqual(store.get_cancellation_receipt(ref, "cancel_rw").outcome, "observed_stop")

    def test_effect_immutable_replay_cas_and_unknown_fence(self):
        for store, job, task, run, ref in self.stores():
            attempt = ExecutionAttempt.from_run(run, adapter="local")
            store.record_attempt(attempt)
            intent = EffectReceipt(ref, "effect_1", immutable_digest({"command": "test"}),
                                   task_binding(task), run.id, attempt.attempt_id, 1,
                                   "not_dispatched", "reconcile_first")
            self.assertEqual(store.record_effect(intent), intent)
            with self.assertRaises(ContractConflict):
                store.record_effect(replace(intent, request_digest=immutable_digest("changed")))
            flight = store.advance_effect(ref, intent.effect_id, expected_revision=1,
                                          outcome="in_flight", evidence_refs=("dispatch:1",))
            reopened = type(store)(store.root)
            self.assertEqual(reopened.record_effect(intent), flight)
            self.assertEqual(reopened.advance_effect(ref, intent.effect_id, expected_revision=1,
                             outcome="in_flight", evidence_refs=("dispatch:1",)), flight)
            unknown = reopened.advance_effect(ref, intent.effect_id, expected_revision=2,
                                              outcome="unknown", evidence_refs=("timeout:1",))
            with self.assertRaises(ContractConflict):
                reopened.advance_effect(ref, intent.effect_id, expected_revision=unknown.revision,
                                        outcome="in_flight", evidence_refs=("retry",))
            result = reopened.advance_effect(ref, intent.effect_id, expected_revision=unknown.revision,
                                             outcome="succeeded", evidence_refs=("reconciled:1",))
            self.assertEqual(result.revision, 4)

    def test_effect_stale_lease_blocks_dispatch(self):
        for store, job, task, run, ref in self.stores():
            attempt = ExecutionAttempt.from_run(run, adapter="local")
            store.record_attempt(attempt)
            intent = EffectReceipt(ref, "effect", immutable_digest({}), task_binding(task), run.id,
                                   attempt.attempt_id, 1, "not_dispatched", "safe")
            store.record_effect(intent)
            store.save_task(replace(task, lease_id="successor"))
            with self.assertRaises(ContractConflict):
                store.advance_effect(ref, "effect", expected_revision=1, outcome="in_flight", evidence_refs=("dispatch",))

    def test_file_crash_index_is_unavailable_until_repaired(self):
        with TemporaryDirectory() as tmp:
            store = SwarmStore(Path(tmp))
            job = store.create_job("before")
            original = store._write_json_file
            def crash(path, value):
                original(path, value)
                raise RuntimeError("crash after rename")
            with patch.object(store, "_write_json_file", side_effect=crash), self.assertRaises(RuntimeError):
                store.save_job(replace(job, label="after"))
            reopened = SwarmStore(Path(tmp))
            self.assertEqual(reopened.list_job_summaries().outcome, "unavailable")
            reopened.repair_metadata_index()
            self.assertEqual(reopened.list_job_summaries().outcome, "complete")

    def test_sqlite_v4_migration_marks_legacy_unknown(self):
        with TemporaryDirectory() as tmp:
            store = SQLiteSwarmStore(Path(tmp))
            job = store.create_job("legacy")
            with closing(sqlite3.connect(store.db_path)) as c, c:
                for row in c.execute("SELECT name FROM sqlite_master WHERE type='trigger'").fetchall():
                    c.execute(f'DROP TRIGGER "{row[0]}"')
                for table in ("projection_current", "projection_changes", "projection_meta", "projection_pending", "contract_receipts", "cancellation_targets"):
                    c.execute(f"DROP TABLE {table}")
                c.execute("UPDATE metadata SET value='4' WHERE key='schema_version'")
            reopened = SQLiteSwarmStore(Path(tmp))
            reopened.init()
            page = reopened.list_job_summaries()
            self.assertEqual(page.items[0].stamp, "legacy_unknown")
            self.assertGreater(page.items[0].revision, 0)
            self.assertEqual(reopened.schema_status()['schema_version'], '7')

    def test_duplicate_store_identity_and_runtime_cancellation(self):
        from puppetmaster.cancellation import cancellation_scope, is_cancelled
        for backend in (SwarmStore, SQLiteSwarmStore):
            with TemporaryDirectory() as tmp:
                a, b = backend(Path(tmp) / 'a'), backend(Path(tmp) / 'b')
                ja = a.create_job('a')
                jb = b.create_job('b')
                if backend == SQLiteSwarmStore:
                    with b._session() as c:
                        c.execute("UPDATE jobs SET id=?, data=? WHERE id=?",
                                  (ja.id, b._dumps(replace(jb, id=ja.id)), jb.id))
                else:
                    b.save_job(replace(jb, id=ja.id))
                ta = Task(job_id=ja.id, role='explore', instruction='a')
                a.save_task(ta)
                ta = a.claim_task(ta.id, 'worker')
                b.save_task(ta)
                ref_a, ref_b = a.job_ref(ja.id), b.job_ref(ja.id)
                a.request_cancellation(ref_a, 'request', [task_binding(ta)])
                with cancellation_scope(b, ta):
                    self.assertFalse(is_cancelled(ja.id))
                with cancellation_scope(a, ta):
                    self.assertTrue(is_cancelled(ja.id))
                with patch('puppetmaster.state.list_project_state_dirs', return_value=[a.root, b.root]):
                    self.assertEqual(resolve_job_state(job_ref=ref_b, cwd=Path(tmp)), b.root)
                    with self.assertRaises(ValueError):
                        resolve_job_state(job_id=ja.id, cwd=Path(tmp))
                    with self.assertRaises(ValueError):
                        resolve_job_state(job_ref=ref_b, state_dir=a.root)
                    with self.assertRaises(ValueError):
                        resolve_job_state(job_ref=ref_b, job_id='other')

    def test_metadata_read_does_not_run_migration(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            with closing(sqlite3.connect(root / "state.sqlite3")) as c, c:
                c.execute("CREATE TABLE jobs(id TEXT PRIMARY KEY, data TEXT)")
                c.execute("INSERT INTO jobs VALUES('old', '{}')")
            store = SQLiteSwarmStore(root)
            with patch.object(store, "ensure_schema", side_effect=AssertionError("migration during read")):
                self.assertEqual(store.list_job_summaries().outcome, "unavailable")

    def test_repair_expires_tokens_and_keeps_filtered_tombstones(self):
        with TemporaryDirectory() as tmp:
            store = SwarmStore(Path(tmp))
            first = store.create_job("first")
            store.create_job("second")
            page = store.list_job_summaries(limit=1)
            store.repair_metadata_index()
            self.assertEqual(store.list_job_summaries(cursor=page.next_cursor).outcome, "cursor_expired")
            revision = store.list_job_summaries().revision
            store.delete_job(first.id)
            changes = store.read_job_summary_changes(after_revision=revision, status=str(first.status))
            self.assertTrue(any(item.deleted and item.id == first.id for item in changes.items))

    def test_projection_counts_bindings_filter_and_byte_limit(self):
        for store, job, task, run, ref in self.stores():
            before = store.list_job_summaries().revision
            for index in range(6):
                store.save_task(Task(job_id=job.id, role="explore", instruction="large" * 10000))
            page = store.list_job_summaries()
            self.assertEqual(page.items[0].task_count, 7)
            self.assertEqual(page.items[0].artifact_count, 0)
            change = store.read_job_summary_changes(after_revision=before)
            self.assertEqual(change.items[-1].task_count, 7)
            tasks = store.list_task_refs(ref, status="running")
            self.assertEqual(tasks.items[0].binding, task_binding(task))
            self.assertEqual(len(tasks.items), 1)
            bounded = store.list_task_refs(ref, max_bytes=1500, max_scan=3)
            self.assertLessEqual(bounded.scanned, 3)
            self.assertLessEqual(len(json.dumps(to_jsonable(bounded)).encode()), 1500)
            with self.assertRaises(ValueError):
                store.list_task_refs(ref, cursor=bounded.next_cursor, status="failed")

    def test_cancelled_queued_generation_does_not_poison_reset(self):
        for store, job, task, run, ref in self.stores():
            queued = replace(task, status=TaskStatus.QUEUED, lease_id=None, lease_owner=None)
            store.save_task(queued)
            store.request_cancellation(ref, "queued", [task_binding(queued)])
            self.assertIsNone(store.claim_task(task.id, "next"))
            store.reset_subgraph(job.id, [task.id])
            claimed = store.claim_task(task.id, "next")
            self.assertIsNotNone(claimed)
            self.assertGreater(claimed.generation, queued.generation)

    def test_execute_effect_is_at_most_one_dispatch_on_exact_replay(self):
        from puppetmaster.contracts import EffectObservation
        for store, job, task, run, ref in self.stores():
            attempt = ExecutionAttempt.from_run(run, adapter="local")
            store.record_attempt(attempt)
            intent = EffectReceipt(ref, "logical_effect", immutable_digest({"command": "fixture"}),
                task_binding(task), run.id, attempt.attempt_id, 1, "not_dispatched", "requires_authorization")
            called = []
            def operation():
                called.append(1)
                return EffectObservation("succeeded", ("fixture:confirmed",))
            receipt = store.execute_effect(intent, operation)
            self.assertEqual(receipt.outcome, "succeeded")
            self.assertEqual(type(store)(store.root).execute_effect(intent, operation), receipt)
            self.assertEqual(called, [1])
            unknown_intent = replace(intent, effect_id="unknown_effect")
            def failed():
                raise OSError("uncertain transport")
            with self.assertRaises(OSError):
                store.execute_effect(unknown_intent, failed)
            self.assertEqual(type(store)(store.root).execute_effect(unknown_intent, operation).outcome, "unknown")
            self.assertEqual(called, [1])

    def test_receipt_keeps_selected_tokens_separate(self):
        from puppetmaster.receipt import build_job_receipt
        from puppetmaster.attempts import UsageObservation
        for store, job, task, run, ref in self.stores():
            attempt = ExecutionAttempt.from_run(run, adapter="local")
            store.record_attempt(attempt)
            store.record_usage_observation(UsageObservation(job.id, attempt.attempt_id, "usage", "fixture",
                now_iso(), usage_state="measured", tokens_in=100, tokens_out=20))
            receipt = build_job_receipt(store, job.id)
            self.assertEqual(receipt["attempt_consumption"]["totals"]["tokens_in"]["total"], 100)
            self.assertNotEqual(receipt["tokens"].get("tokens_in"), 100)
            json.dumps(receipt)


class OwnedCancellationCleanup(unittest.TestCase):
    def test_reaped_root_never_signals_unrelated_sentinel(self):
        import subprocess
        import sys
        import time
        from unittest.mock import Mock
        from puppetmaster.win_process import cleanup_owned_process
        sentinel = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
        try:
            stale = Mock(pid=sentinel.pid)
            stale.poll.return_value = 0
            with patch("puppetmaster.win_process._owned_posix_pids", return_value=[]), \
                 patch("puppetmaster.win_process.os.killpg", create=True) as group:
                result = cleanup_owned_process(stale, "expired-owner", time.monotonic() + 1)
            stale.kill.assert_not_called()
            group.assert_not_called()
            self.assertIsNone(sentinel.poll())
            self.assertEqual(result.remote_effects, "unknown")
        finally:
            sentinel.kill()
            sentinel.wait(timeout=5)

    def test_real_scoped_cancellation_interrupts_streamed_child(self):
        import sys
        import threading
        import time
        from puppetmaster.adapters._streaming import run_streamed_subprocess
        from puppetmaster.cancellation import cancellation_scope, JobCancelled
        with TemporaryDirectory() as tmp:
            store = SQLiteSwarmStore(Path(tmp))
            job = store.create_job("cancel process")
            task = Task(job_id=job.id, role="explore", instruction="fixture")
            store.save_task(task)
            task = store.claim_task(task.id, "worker")
            ref = store.job_ref(job.id)
            errors = []
            def run():
                try:
                    with cancellation_scope(store, task):
                        run_streamed_subprocess(command=[sys.executable, "-c", "import time; time.sleep(30)"],
                            env=None, task=task, sidecar_name="test", timeout_seconds=30)
                except JobCancelled:
                    errors.append("cancelled")
            worker = threading.Thread(target=run)
            worker.start()
            time.sleep(.1)
            store.request_cancellation(ref, "stop", [task_binding(task)])
            worker.join(timeout=8)
            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, ["cancelled"])
            self.assertEqual(store.get_cancellation_receipt(ref, "stop").outcome, "observed_stop")


class ReadOnlyFeatureRegression(unittest.TestCase):
    def test_feature_respects_non_edit_payloads_and_explicit_gates(self):
        from puppetmaster.playbooks import stamp_payload
        for flags in ({'read_only': True}, {'no_edit': True}, {'dry_run': True},
                      {'swarm_mode': 'analysis'}, {'edit_mode': 'no-edit'}):
            self.assertNotIn('gates', stamp_payload(flags, 'feature'))
            explicit = {**flags, 'gates': [{'kind': 'require_diff'}, {'kind': 'command', 'command': 'test'}]}
            self.assertEqual(stamp_payload(explicit, 'feature')['gates'], explicit['gates'])
        self.assertEqual(stamp_payload({'mode': 'implement'}, 'feature')['gates'], [{'kind': 'require_diff'}])
