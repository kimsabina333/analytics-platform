"""
Persistent SQLite store for:
  - users (auth)
  - cache  (BQ raw data + prediction results; zlib-compressed JSON)

Cache strategy
--------------
L1  in-memory dict      hot, < 5 min, lost on restart
L2  Redis               fast, < 5 min, lost on restart (optional)
L3  SQLite (this file)  persistent, 24 h TTL, survives restarts

Why 24 h?
  - BQ data reflects "yesterday"; the same calendar date never changes.
  - Prediction keys include date_max, so stale results are naturally bypassed
    when the date advances (different key → cache miss → fresh compute).
  - LTV model weights don't change intraday.

Compression
-----------
Raw BQ DataFrames as JSON can be 5-20 MB.
zlib level-6 typically shrinks them 5-8×, so we store 1-4 MB per entry.
"""

import json
import sqlite3
import time
import zlib
from pathlib import Path
from typing import Any, Optional

DB_PATH = Path(__file__).parent.parent / "app.db"

_DDL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;

CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    email         TEXT    UNIQUE NOT NULL,
    password_hash TEXT    NOT NULL,
    created_at    REAL    NOT NULL
);

CREATE TABLE IF NOT EXISTS cache (
    key        TEXT    PRIMARY KEY,
    payload    BLOB    NOT NULL,
    expires_at REAL    NOT NULL,
    created_at REAL    NOT NULL,
    size_kb    INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_cache_exp ON cache (expires_at);

CREATE TABLE IF NOT EXISTS segment_predictions (
    model          TEXT    NOT NULL,
    segment_key    TEXT    NOT NULL,
    date_max       TEXT    NOT NULL,
    dimension      TEXT    NOT NULL,
    value          TEXT    NOT NULL,
    q_threshold    REAL    NOT NULL,
    is_alert       INTEGER NOT NULL,
    latest_sr      REAL,
    latest_count   INTEGER NOT NULL DEFAULT 0,
    payload        BLOB    NOT NULL,
    updated_at     REAL    NOT NULL,
    size_kb        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (model, segment_key, date_max)
);

CREATE INDEX IF NOT EXISTS idx_segment_predictions_latest
    ON segment_predictions (model, segment_key, date_max DESC);
CREATE INDEX IF NOT EXISTS idx_segment_predictions_model_date
    ON segment_predictions (model, date_max DESC);
"""


class DBService:
    def __init__(self, db_path: Path = DB_PATH):
        self._path = str(db_path)
        self._init()

    # ── internals ─────────────────────────────────────────────────────────────

    def _conn(self) -> sqlite3.Connection:
        con = sqlite3.connect(self._path, timeout=10)
        con.execute("PRAGMA journal_mode = WAL")
        con.execute("PRAGMA synchronous  = NORMAL")
        return con

    def _init(self) -> None:
        con = self._conn()
        con.executescript(_DDL)
        con.commit()
        con.close()
        # Remove stale entries left from previous run
        self.cache_evict_expired()

    # ── cache ─────────────────────────────────────────────────────────────────

    def cache_get(self, key: str) -> Optional[str]:
        """Return the raw JSON string for *key*, or None if missing/expired."""
        con = self._conn()
        try:
            row = con.execute(
                "SELECT payload, expires_at FROM cache WHERE key = ?", (key,)
            ).fetchone()
        finally:
            con.close()

        if row is None:
            return None
        payload, exp = row
        if exp < time.time():
            self.cache_delete(key)
            return None
        return zlib.decompress(payload).decode()

    def cache_set(self, key: str, value_str: str, ttl: int = 86400) -> None:
        """Compress *value_str* and persist it with *ttl* seconds until expiry."""
        compressed = zlib.compress(value_str.encode(), level=6)
        size_kb = max(1, len(compressed) // 1024)
        exp = time.time() + ttl
        con = self._conn()
        try:
            con.execute(
                "INSERT OR REPLACE INTO cache (key, payload, expires_at, created_at, size_kb)"
                " VALUES (?, ?, ?, ?, ?)",
                (key, compressed, exp, time.time(), size_kb),
            )
            con.commit()
        finally:
            con.close()

    def cache_delete(self, key: str) -> None:
        con = self._conn()
        try:
            con.execute("DELETE FROM cache WHERE key = ?", (key,))
            con.commit()
        finally:
            con.close()

    def cache_delete_pattern(self, prefix: str) -> int:
        """Delete all keys whose name starts with *prefix*."""
        con = self._conn()
        try:
            cur = con.execute(
                "DELETE FROM cache WHERE key LIKE ?", (prefix.replace("*", "%"),)
            )
            con.commit()
            return cur.rowcount
        finally:
            con.close()

    def cache_evict_expired(self) -> int:
        """Prune expired rows; returns the number of rows removed."""
        con = self._conn()
        try:
            cur = con.execute("DELETE FROM cache WHERE expires_at < ?", (time.time(),))
            con.commit()
            return cur.rowcount
        finally:
            con.close()

    def cache_stats(self) -> dict:
        """Summary of what is currently stored (for /api/admin/cache)."""
        con = self._conn()
        try:
            row = con.execute(
                "SELECT COUNT(*), COALESCE(SUM(size_kb),0) FROM cache WHERE expires_at > ?",
                (time.time(),),
            ).fetchone()
            keys = con.execute(
                "SELECT key, size_kb, expires_at FROM cache WHERE expires_at > ?"
                " ORDER BY expires_at DESC LIMIT 50",
                (time.time(),),
            ).fetchall()
        finally:
            con.close()

        return {
            "entries": row[0],
            "total_size_kb": row[1],
            "items": [
                {
                    "key": k,
                    "size_kb": s,
                    "expires_in_min": round((e - time.time()) / 60),
                }
                for k, s, e in keys
            ],
        }

    # ── materialized segment predictions ─────────────────────────────────

    def segment_prediction_get_latest(
        self, model: str, segment_key: str
    ) -> Optional[str]:
        """Return the newest materialized prediction JSON for one segment."""
        con = self._conn()
        try:
            row = con.execute(
                """
                SELECT payload
                FROM segment_predictions
                WHERE model = ? AND segment_key = ?
                ORDER BY date_max DESC
                LIMIT 1
                """,
                (model, segment_key),
            ).fetchone()
        finally:
            con.close()

        if row is None:
            return None
        return zlib.decompress(row[0]).decode()

    def segment_prediction_set(
        self,
        model: str,
        segment_key: str,
        date_max: str,
        dimension: str,
        value: str,
        q_threshold: float,
        is_alert: bool,
        latest_sr: Optional[float],
        latest_count: int,
        value_str: str,
    ) -> None:
        """Persist a ready-to-serve SegmentPredictionResult payload."""
        compressed = zlib.compress(value_str.encode(), level=6)
        size_kb = max(1, len(compressed) // 1024)
        con = self._conn()
        try:
            con.execute(
                """
                INSERT OR REPLACE INTO segment_predictions (
                    model, segment_key, date_max, dimension, value, q_threshold,
                    is_alert, latest_sr, latest_count, payload, updated_at, size_kb
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    model,
                    segment_key,
                    date_max,
                    dimension,
                    value,
                    q_threshold,
                    1 if is_alert else 0,
                    latest_sr,
                    latest_count,
                    compressed,
                    time.time(),
                    size_kb,
                ),
            )
            con.commit()
        finally:
            con.close()

    def segment_prediction_list_latest(
        self, model: str, segment_keys: list[str]
    ) -> list[str]:
        """Return newest materialized payloads for the requested segment keys."""
        if not segment_keys:
            return []

        con = self._conn()
        try:
            result = []
            for key in segment_keys:
                row = con.execute(
                    """
                    SELECT payload
                    FROM segment_predictions
                    WHERE model = ? AND segment_key = ?
                    ORDER BY date_max DESC
                    LIMIT 1
                    """,
                    (model, key),
                ).fetchone()
                if row is not None:
                    result.append(zlib.decompress(row[0]).decode())
            return result
        finally:
            con.close()

    def segment_prediction_stats(self) -> dict:
        con = self._conn()
        try:
            row = con.execute(
                "SELECT COUNT(*), COALESCE(SUM(size_kb),0) FROM segment_predictions"
            ).fetchone()
            keys = con.execute(
                """
                SELECT model, segment_key, date_max, size_kb, updated_at
                FROM segment_predictions
                ORDER BY updated_at DESC
                LIMIT 50
                """
            ).fetchall()
        finally:
            con.close()

        return {
            "entries": row[0],
            "total_size_kb": row[1],
            "items": [
                {
                    "model": model,
                    "segment_key": key,
                    "date_max": date_max,
                    "size_kb": size_kb,
                    "updated_at": updated_at,
                }
                for model, key, date_max, size_kb, updated_at in keys
            ],
        }

    # ── users (mirrors what auth.py needs) ────────────────────────────────────

    def user_create(self, email: str, password_hash: str) -> int:
        con = self._conn()
        try:
            cur = con.execute(
                "INSERT INTO users (email, password_hash, created_at) VALUES (?,?,?)",
                (email, password_hash, time.time()),
            )
            con.commit()
            return cur.lastrowid
        finally:
            con.close()

    def user_by_email(self, email: str) -> Optional[dict]:
        con = self._conn()
        try:
            row = con.execute(
                "SELECT id, email, password_hash FROM users WHERE email = ?", (email,)
            ).fetchone()
        finally:
            con.close()
        if row is None:
            return None
        return {"id": row[0], "email": row[1], "password_hash": row[2]}

    def user_update_password(self, user_id: int, password_hash: str) -> None:
        con = self._conn()
        try:
            con.execute(
                "UPDATE users SET password_hash = ? WHERE id = ?",
                (password_hash, user_id),
            )
            con.commit()
        finally:
            con.close()
