"""Bitácora acumulativa local: inserciones únicas, hashes y bloqueo SQL de cambios."""
import hashlib
import json
import sqlite3
from main import ROOT


def connection():
    path = ROOT / "data/audit/events.sqlite"
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS events (
          id INTEGER PRIMARY KEY, run_id TEXT UNIQUE NOT NULL,
          payload TEXT NOT NULL, previous_hash TEXT NOT NULL, event_hash TEXT NOT NULL);
        CREATE TRIGGER IF NOT EXISTS no_update BEFORE UPDATE ON events
          BEGIN SELECT RAISE(ABORT, 'append-only: UPDATE bloqueado'); END;
        CREATE TRIGGER IF NOT EXISTS no_delete BEFORE DELETE ON events
          BEGIN SELECT RAISE(ABORT, 'append-only: DELETE bloqueado'); END;
    """)
    return db


def append(record):
    with connection() as db:
        db.execute("BEGIN IMMEDIATE")
        if db.execute("SELECT 1 FROM events WHERE run_id=?", (record["mlflow_run_id"],)).fetchone():
            return
        last = db.execute("SELECT event_hash FROM events ORDER BY id DESC LIMIT 1").fetchone()
        previous = last[0] if last else "0" * 64
        payload = json.dumps(record, ensure_ascii=False, sort_keys=True)
        digest = hashlib.sha256((previous + payload).encode()).hexdigest()
        db.execute("INSERT INTO events(run_id,payload,previous_hash,event_hash) VALUES(?,?,?,?)",
                   (record["mlflow_run_id"], payload, previous, digest))


def records():
    with connection() as db:
        return [{**json.loads(payload), "Previous_Hash": previous, "Event_Hash": digest}
                for payload, previous, digest in db.execute(
                    "SELECT payload,previous_hash,event_hash FROM events ORDER BY id")]
