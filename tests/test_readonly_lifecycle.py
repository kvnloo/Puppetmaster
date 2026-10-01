"""Descriptor ownership at ready, retry, and failed teardown boundaries."""
import gc
import json
import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from puppetmaster import readonly
from puppetmaster.sqlite_store import SQLiteSwarmStore


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = SQLiteSwarmStore(self.directory.name)
        self.store.ensure_schema()
        self.before = set(readonly._cleanup.owners)
        self.addCleanup(self.cleanup)

    def cleanup(self):
        for token in set(readonly._cleanup.owners) - self.before:
            readonly._cleanup.close(token)

    def test_ready_precedes_admission_and_open(self):
        events = []
        ready = readonly._Transport.ready
        admission = readonly.ReaderAdmission
        def prepared(transport, deadline):
            ready(transport, deadline)
            events.append('ready')
        def admitted(*args, **kwargs):
            self.assertEqual(events, ['ready'])
            events.append('admission')
            return admission(*args, **kwargs)
        with patch.object(readonly._Transport, 'ready', prepared), \
                patch.object(readonly, 'ReaderAdmission', admitted):
            with readonly.connect(self.store) as connection:
                self.assertEqual(connection.execute('SELECT 42').fetchone()[0], 42)
                events.append('query')
        self.assertEqual(events, ['ready', 'admission', 'query'])

    def test_finalizer_registration_and_teardown_failure_remain_owned(self):
        with patch.object(readonly.weakref, 'finalize', side_effect=RuntimeError('registration failed')), \
                patch.object(readonly.ReaderProcess, 'terminate', side_effect=OSError('terminate failed')):
            with self.assertRaisesRegex(OSError, 'terminate failed'):
                readonly.connect(self.store, reuse=True)
        tokens = set(readonly._cleanup.owners) - self.before
        self.assertEqual(len(tokens), 1)
        token = tokens.pop()
        owner = readonly._cleanup.owners[token]
        self.assertTrue(owner.retired)
        self.assertIsNone(owner.permit)
        self.assertIsNone(self.store._readonly_slot.transport)
        readonly._cleanup.close(token)
        self.assertTrue(owner.transport.closed)
        with readonly.connect(self.store, reuse=True) as later:
            self.assertEqual(later.execute('SELECT 42').fetchone()[0], 42)

    def test_full_multi_message_queue_and_eof_do_not_strand_reader(self):
        transport = readonly._Transport(self.store.db_path)
        self.assertTrue(transport.responses.not_empty.acquire(timeout=1))
        transport.responses.not_empty.release()
        # Keep readiness in the bounded queue while more output and EOF arrive.
        transport.process.stdin.write(json.dumps({'open': str(self.store.db_path)}) + '\n')
        transport.process.stdin.write(json.dumps({'control': 'release', 'value': True}) + '\n')
        transport.process.stdin.flush()
        transport.process.stdin.close()
        started = time.monotonic()
        readonly._cleanup.close(transport.token)
        self.assertLess(time.monotonic() - started, 1)
        self.assertEqual(transport.state, readonly._TransportState.CLOSED)
        self.assertIsNotNone(transport.process.poll())
        self.assertFalse(transport.reader.is_alive())
        self.assertTrue(transport.reader_exited.is_set())

    def test_reaped_helper_with_exited_reader_closes_on_a_spent_budget(self):
        # CI shape: terminate+wait spends the whole teardown budget, and the
        # reader has run its finally (no pipe access left) but has not yet
        # returned. That is a completed teardown, not "thread remains alive".
        transport = readonly._Transport(self.store.db_path)
        transport.ready(time.monotonic() + 5)
        lingering = threading.Event()
        class LingeringExit(threading.Event):
            def set(event):
                super().set()
                lingering.wait(5)
        transport.reader_exited = LingeringExit()
        self.addCleanup(lingering.set)
        transport.process.kill()
        transport.process.wait(5)
        self.assertTrue(transport.reader_exited.wait(5))
        self.assertTrue(transport.reader.is_alive())
        transport.close(deadline=time.monotonic())
        self.assertEqual(transport.state, readonly._TransportState.CLOSED)
        readonly._cleanup.retire(transport.token)

    def test_partial_native_thread_start_cannot_report_closed(self):
        if not hasattr(threading, '_start_new_thread'):
            self.skipTest('CPython 3.9/3.12 thread startup seam')
        gate = threading.Event()
        native_start = threading._start_new_thread
        def partial(function, args):
            def delayed():
                # Only the test releases startup, after checking cleanup ownership.
                gate.wait()
                try:
                    function(*args)
                except KeyError:
                    pass  # Thread.start removed the partially launched thread from _limbo.
            native_start(delayed, ())
            raise RuntimeError('native launch then raise')
        try:
            with patch.object(threading, '_start_new_thread', partial):
                with self.assertRaisesRegex(readonly.ReadUnavailable, 'thread exit unconfirmed'):
                    readonly.connect(self.store, timeout=.2)
            tokens = set(readonly._cleanup.owners) - self.before
            self.assertEqual(len(tokens), 1)
            token = tokens.pop()
            owner = readonly._cleanup.owners[token]
            self.assertTrue(owner.retired)
            self.assertIsNone(owner.permit)
            self.assertEqual(owner.transport.state, readonly._TransportState.REAPED)
            self.assertFalse(owner.transport.closed)
        finally:
            gate.set()
        readonly._cleanup.close(token)
        self.assertTrue(owner.transport.closed)
        self.assertFalse(owner.transport.reader.is_alive())
        self.assertNotIn(token, readonly._cleanup.owners)

    def test_process_launch_then_raise_retains_preallocated_handle(self):
        initialize = readonly.ReaderProcess.__init__
        def partial(process, *args, **kwargs):
            initialize(process, *args, **kwargs)
            raise OSError('process launch then raise')
        with patch.object(readonly.ReaderProcess, '__init__', partial):
            with self.assertRaisesRegex(OSError, 'process launch then raise'):
                readonly.connect(self.store)
        tokens = set(readonly._cleanup.owners) - self.before
        self.assertEqual(len(tokens), 1)
        token = tokens.pop()
        owner = readonly._cleanup.owners[token]
        self.assertIsNotNone(owner.transport.process.pid)
        self.assertIsNone(owner.permit)
        self.assertFalse(owner.transport.closed)
        readonly._cleanup.close(token)
        self.assertIsNotNone(owner.transport.process.poll())
        self.assertTrue(owner.transport.closed)

    def test_attach_backoff_releases_admission(self):
        receive = readonly.ReadConnection._receive
        calls = []
        def fail_once(connection):
            response = receive(connection)
            if not connection._opened and not calls:
                connection._opened = True
                connection._control('release', True)
                connection._opened = False
                calls.append(connection.transport)
                error = readonly.ReadUnavailable('unable to open database: source changed')
                error.same_store_write = True
                error.session_closed = True
                raise error
            return response
        sleep = time.sleep
        backoffs = []
        def check_backoff(delay):
            permit = readonly.ReaderAdmission(self.store.db_path, time.monotonic() + .1)
            permit.release()
            backoffs.append(delay)
            sleep(delay)
        with patch.object(readonly.ReadConnection, '_receive', fail_once), \
                patch.object(readonly, 'time', __import__('types').SimpleNamespace(
                    monotonic=time.monotonic, sleep=check_backoff)):
            with readonly.connect(self.store, attach_binding=True) as connection:
                self.assertIs(connection.transport, calls[0])
        self.assertEqual(len(backoffs), 1)

    def test_close_expiry_keeps_permit_and_truthful_state(self):
        from subprocess import TimeoutExpired
        from types import SimpleNamespace
        connection = readonly.connect(self.store)
        self.addCleanup(connection.close)
        transport = connection.transport
        now = [0.0]
        waits = []
        def wait(timeout):
            waits.append(timeout)
            now[0] += timeout
            raise TimeoutExpired('reader', timeout)
        with patch.object(readonly, 'time', SimpleNamespace(monotonic=lambda: now[0])), \
                patch.object(transport.process, 'terminate', return_value=None), \
                patch.object(transport.process, 'kill', return_value=None), \
                patch.object(transport.process, 'wait', wait):
            with self.assertRaises(readonly.ReadTimeout):
                readonly._cleanup.close(connection._token, deadline=.03)
        self.assertEqual(waits, [.03, 0])
        self.assertFalse(transport.closed)
        self.assertEqual(transport.state, readonly._TransportState.CLOSING)
        self.assertIsNotNone(readonly._cleanup.owners[connection._token].permit)
        self.assertTrue(transport.close_event.is_set())
        self.assertIsNone(transport.process.poll())
        readonly._cleanup.close(connection._token)
        self.assertTrue(transport.closed)
