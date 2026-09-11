"""Durable HTTP submission identities, independent of expiring run history."""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import uuid


class RequestConflict(ValueError):
    pass


class RequestBusy(RuntimeError):
    pass


class RequestJournal:
    def __init__(self, root: Path):
        self.root = root / '_submission_requests'
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.database = self.root / 'journal.sqlite3'
        with self.connect() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS requests (
                key TEXT PRIMARY KEY, digest TEXT NOT NULL, payload TEXT NOT NULL,
                run_id TEXT NOT NULL UNIQUE, phase TEXT NOT NULL,
                retain INTEGER NOT NULL, archive_receipt TEXT)''')
        os.chmod(self.database, 0o600)
        fd = os.open(self.root, os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.database, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def validate_key(key):
        if not isinstance(key, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_./:-]{0,199}', key):
            raise ValueError('Invalid request_key')

    @contextmanager
    def lease(self, key):
        self.validate_key(key)
        name = hashlib.sha256(key.encode()).hexdigest()
        with (self.root / (name + '.lock')).open('a') as stream:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RequestBusy('Request is being processed; retry the same key') from exc
            yield

    def reserve(self, key, payload):
        self.validate_key(key)
        canonical = json.dumps(payload, sort_keys=True, separators=(',', ':'), allow_nan=False)
        checksum = hashlib.sha256(canonical.encode()).hexdigest()
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM requests WHERE key=?', (key,)).fetchone()
            if row:
                if row['digest'] != checksum:
                    raise RequestConflict('request_key already belongs to a different payload')
            else:
                db.execute('INSERT INTO requests VALUES (?,?,?,?,?,?,NULL)',
                           (key, checksum, canonical, uuid.uuid4().hex, 'reserved',
                            int(payload.get('retain_until_archived', False))))
        return self.lookup(key)

    def lookup(self, key):
        self.validate_key(key)
        with self.connect() as db:
            row = db.execute('SELECT * FROM requests WHERE key=?', (key,)).fetchone()
        if row is None:
            return None
        return dict(row)

    def accepted(self, key):
        with self.connect() as db:
            db.execute("UPDATE requests SET phase='accepted' WHERE key=?", (key,))

    def acknowledge(self, key, receipt):
        if not isinstance(receipt, str) or not re.fullmatch('[a-f0-9]{64}', receipt):
            raise ValueError('archive_receipt_sha256 must be a SHA-256')
        with self.connect() as db:
            row = db.execute('SELECT phase,archive_receipt FROM requests WHERE key=?', (key,)).fetchone()
            if row is None or row['phase'] != 'accepted':
                raise ValueError('Request has not been accepted')
            if row['archive_receipt'] not in (None, receipt):
                raise RequestConflict('A different archive receipt was already acknowledged')
            db.execute('UPDATE requests SET archive_receipt=? WHERE key=?', (receipt, key))

    def protected(self, run_id):
        with self.connect() as db:
            return db.execute('SELECT 1 FROM requests WHERE run_id=? AND retain=1 AND archive_receipt IS NULL',
                              (run_id,)).fetchone() is not None

    def pending(self):
        with self.connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM requests WHERE phase='reserved'")]
