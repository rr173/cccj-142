"""SQLite backing store for one run: spill mailbox, progress log, seen ids.

This store holds *runtime* data only: the overflow mailbox for feedback
edges (bounded-memory spill), the progress event log of this run, and the
caller-supplied message ids seen so far.  It is deliberately NOT a recovery
protocol: tables are dropped and recreated at the start of every run, and if
the process exits the run is considered failed — nothing here is meant to be
resumed by a later job.
"""

import os
import pickle
import sqlite3
import tempfile
import threading
import time

_SCHEMA = """
CREATE TABLE spill(
    edge_id TEXT NOT NULL,
    seq     INTEGER NOT NULL,
    msg     BLOB NOT NULL,
    PRIMARY KEY(edge_id, seq)
);
CREATE TABLE progress_events(
    seq    INTEGER PRIMARY KEY AUTOINCREMENT,
    ts     REAL NOT NULL,
    kind   TEXT NOT NULL,
    epoch  INTEGER,
    round  INTEGER,
    detail TEXT
);
CREATE TABLE seen_ids(
    source_id TEXT NOT NULL,
    msg_id    TEXT NOT NULL,
    PRIMARY KEY(source_id, msg_id)
);
CREATE TABLE run_meta(
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


class Database:
    def __init__(self, path=None):
        self._tmp_path = None
        if path is None:
            fd, path = tempfile.mkstemp(prefix="iterflow-", suffix=".db")
            os.close(fd)
            self._tmp_path = path
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        with self._lock:
            self._conn.executescript(
                "DROP TABLE IF EXISTS spill;"
                "DROP TABLE IF EXISTS progress_events;"
                "DROP TABLE IF EXISTS seen_ids;"
                "DROP TABLE IF EXISTS run_meta;"
            )
            self._conn.executescript(_SCHEMA)
            self._conn.execute(
                "INSERT INTO run_meta(key, value) VALUES('started_ts', ?)",
                (repr(time.time()),),
            )
            self._conn.commit()

    # ---------------------------------------------------------- spill mailbox
    def spill_put(self, edge_id, seq, msg):
        blob = pickle.dumps(msg, protocol=4)
        with self._lock:
            self._conn.execute(
                "INSERT INTO spill(edge_id, seq, msg) VALUES(?,?,?)",
                (edge_id, seq, blob),
            )
            self._conn.commit()

    def spill_take(self, edge_id):
        """Pop the oldest spilled message of an edge, or None."""
        with self._lock:
            row = self._conn.execute(
                "SELECT seq, msg FROM spill WHERE edge_id=? ORDER BY seq LIMIT 1",
                (edge_id,),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "DELETE FROM spill WHERE edge_id=? AND seq=?", (edge_id, row[0])
            )
            self._conn.commit()
        return pickle.loads(row[1])

    def spill_depth(self, edge_id):
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM spill WHERE edge_id=?", (edge_id,)
            ).fetchone()
        return row[0]

    # ------------------------------------------------------------- seen ids
    def seen_insert(self, source_id, msg_id):
        """Record a caller-supplied message id; False if it already existed."""
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO seen_ids(source_id, msg_id) VALUES(?,?)",
                    (source_id, msg_id),
                )
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    # ----------------------------------------------------------- progress log
    def log(self, kind, epoch=None, round=None, detail=""):
        with self._lock:
            self._conn.execute(
                "INSERT INTO progress_events(ts, kind, epoch, round, detail)"
                " VALUES(?,?,?,?,?)",
                (time.time(), kind, epoch, round, detail),
            )
            self._conn.commit()

    def events(self):
        with self._lock:
            rows = self._conn.execute(
                "SELECT seq, ts, kind, epoch, round, detail"
                " FROM progress_events ORDER BY seq"
            ).fetchall()
        return [
            {"seq": r[0], "ts": r[1], "kind": r[2], "epoch": r[3],
             "round": r[4], "detail": r[5]}
            for r in rows
        ]

    def set_meta(self, key, value):
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO run_meta(key, value) VALUES(?,?)",
                (key, value),
            )
            self._conn.commit()

    def close(self):
        with self._lock:
            self._conn.close()
