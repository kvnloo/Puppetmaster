"""Real damaged-WAL fixtures on both POSIX and mandatory-lock Windows filesystems."""
from contextlib import contextmanager, closing
import mmap
import os
from pathlib import Path
import sqlite3


def file_bytes(path):
    # Windows byte-range locks prohibit ReadFile, including SHM's lock bytes.
    # A read-only mapping can inspect the complete fixture without writing it.
    if os.name == 'nt' and path.stat().st_size:
        with path.open('rb') as source, mmap.mmap(source.fileno(), 0, access=mmap.ACCESS_READ) as data:
            return data[:]
    return path.read_bytes()


@contextmanager
def damaged_sidecars(writer, path, suffixes=('-wal', '-shm')):
    """Hide real sidecars, retaining a main-file reader lock until restoration.

    SQLite's Windows handles deny delete sharing. Detach a byte-exact fixture
    from the writer before damaging it, then hold SQLite's shared lock range
    ourselves. The source remains unavailable even when both sidecars vanish.
    Recovery happens only on fixture teardown, never in the tested reader.
    """
    path = Path(path)
    lock = None
    pairs = [(Path(str(path) + suffix), Path(str(path) + suffix + '.hidden')) for suffix in suffixes]
    if os.name == 'nt':
        import msvcrt
        files = [Path(str(path) + suffix) for suffix in ('', '-wal', '-shm')]
        snapshot = {p: file_bytes(p) for p in files if p.exists()}
        writer.close()
        for p, data in snapshot.items():
            p.write_bytes(data)
        lock = path.open('rb')
        lock.seek(1073741826)
        msvcrt.locking(lock.fileno(), msvcrt.LK_NBRLCK, 510)
    moved = []
    try:
        for source, target in pairs:
            source.rename(target)
            moved.append((source, target))
        yield
    finally:
        for source, target in reversed(moved):
            target.rename(source)
        if lock is not None:
            lock.close()
            with closing(sqlite3.connect(path)) as recovered:
                recovered.execute('SELECT * FROM sqlite_master').fetchall()


def replacement_blocked(operation):
    """Return true only for Windows' explicit open-file replacement protection."""
    try:
        operation()
    except PermissionError as exc:
        if os.name != 'nt' or exc.winerror not in (5, 32):
            raise
        return True
    return False


class ProtocolTransport:
    """Deterministic open/query responses after a successful helper handshake."""
    def __init__(self, path):
        from puppetmaster import readonly
        self.token = readonly._cleanup.register(self, (str(path),))

    def ready(self, deadline):
        pass


def close_new_cleanup_owners(test):
    """Close helpers a test left in the process-wide cleanup registry.

    Fault-injection tests retain owners on purpose. Left behind, they are what
    the next read's one-owner idle sweep reaps, not the caller's own helper.
    """
    from puppetmaster import readonly
    before = set(readonly._cleanup.owners)

    def close():
        for token in set(readonly._cleanup.owners) - before:
            readonly._cleanup.close(token)
    test.addCleanup(close)
