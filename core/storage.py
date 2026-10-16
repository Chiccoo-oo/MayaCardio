"""
High-performance, crash-safe SQLite WAL storage for raw samples, lost records, and window results.
Guarantees durability across process crashes, reboots, and power cuts.
"""

import sqlite3
import json
import os
import threading
import time
from contextlib import contextmanager
from typing import Dict, List, Optional, Any, Tuple, Generator, Sequence

RESERVE_FILE_NAME = "maya_disk_reserve.dat"
RESERVE_SIZE_BYTES = 1024 * 1024  # 1 MB emergency reserve

class DeviceStorage:
    def __init__(self, db_path: str = "maya_device.db"):
        self.db_path = db_path
        self._lock = threading.RLock()
        self._init_reserve()
        self._init_db()

    def _init_reserve(self):
        """Preallocate emergency disk space to allow writing lost records if disk fills up."""
        try:
            db_dir = os.path.dirname(os.path.abspath(self.db_path))
            reserve_path = os.path.join(db_dir, RESERVE_FILE_NAME)
            if not os.path.exists(reserve_path):
                with open(reserve_path, "wb") as f:
                    f.write(b"\0" * RESERVE_SIZE_BYTES)
        except Exception:
            pass

    def release_reserve(self):
        """Release the emergency reserve file if disk is full."""
        try:
            db_dir = os.path.dirname(os.path.abspath(self.db_path))
            reserve_path = os.path.join(db_dir, RESERVE_FILE_NAME)
            if os.path.exists(reserve_path):
                os.remove(reserve_path)
        except Exception:
            pass

    @contextmanager
    def _connection(self):
        conn = sqlite3.connect(self.db_path, timeout=60.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _init_db(self):
        with self._lock, self._connection() as conn:
            # WAL mode for high concurrency; FULL synchronous for zero loss on sudden power cut
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = FULL;")
            conn.execute("PRAGMA temp_store = MEMORY;")
            conn.execute("PRAGMA busy_timeout = 60000;")
            
            # Sessions table with implicit rowid used for strict monotonically increasing order
            conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                started_at REAL,
                is_active INTEGER DEFAULT 1,
                ended INTEGER DEFAULT 0
            );
            """)
            
            # Samples table
            conn.execute("""
            CREATE TABLE IF NOT EXISTS samples (
                session_id TEXT,
                sensor INTEGER,
                sample_index INTEGER,
                val1 INTEGER,
                val2 INTEGER,
                val3 INTEGER,
                synced INTEGER DEFAULT 0,
                PRIMARY KEY (session_id, sensor, sample_index)
            ) WITHOUT ROWID;
            """)
            
            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_samples_sync 
            ON samples (session_id, synced, sensor, sample_index);
            """)
            
            # Lost ranges table with unique constraint to prevent duplicate loss records
            conn.execute("""
            CREATE TABLE IF NOT EXISTS lost_ranges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT,
                sensor INTEGER,
                from_index INTEGER,
                to_index INTEGER,
                reason TEXT,
                synced INTEGER DEFAULT 0
            );
            """)
            
            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_lost_sync
            ON lost_ranges (session_id, synced);
            """)
            
            conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_lost_unique
            ON lost_ranges (session_id, sensor, from_index, IFNULL(to_index, -1));
            """)
            
            # Reader cursor state table: tracks exact read position on disk per session and sensor
            conn.execute("""
            CREATE TABLE IF NOT EXISTS reader_state (
                session_id TEXT,
                sensor INTEGER,
                last_index INTEGER,
                updated_at REAL,
                PRIMARY KEY (session_id, sensor)
            ) WITHOUT ROWID;
            """)
            
            # Results table: supports both 'online' and 'offline' results per window
            conn.execute("""
            CREATE TABLE IF NOT EXISTS results (
                session_id TEXT,
                window INTEGER,
                mode TEXT,
                result_json TEXT,
                processed_at REAL,
                synced INTEGER DEFAULT 0,
                PRIMARY KEY (session_id, window, mode)
            ) WITHOUT ROWID;
            """)
            
            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_results_sync
            ON results (session_id, synced);
            """)
            
            # Key-Value metadata store for daemon status, mode, and counters
            conn.execute("""
            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                val TEXT,
                updated_at REAL
            ) WITHOUT ROWID;
            """)
            
            conn.commit()

    # --- Session & Reader State Management ---

    def register_session(self, session_id: str, started_at: Optional[float] = None):
        if started_at is None:
            started_at = time.time()
        with self._lock, self._connection() as conn:
            conn.execute("""
            INSERT OR IGNORE INTO sessions (session_id, started_at, is_active, ended)
            VALUES (?, ?, 1, 0);
            """, (session_id, started_at))
            conn.commit()

    def mark_session_ended(self, session_id: str):
        with self._lock, self._connection() as conn:
            conn.execute("""
            UPDATE sessions SET is_active = 0, ended = 1 WHERE session_id = ?;
            """, (session_id,))
            conn.commit()

    def get_active_session(self) -> Optional[str]:
        with self._lock, self._connection() as conn:
            cursor = conn.execute("""
            SELECT session_id FROM sessions 
            WHERE is_active = 1 
            ORDER BY rowid DESC LIMIT 1;
            """)
            row = cursor.fetchone()
            if row:
                return row["session_id"]
            # Fall back to latest session if none active
            cursor = conn.execute("""
            SELECT session_id FROM sessions 
            ORDER BY rowid DESC LIMIT 1;
            """)
            row = cursor.fetchone()
            return row["session_id"] if row else None

    def get_latest_session_info(self) -> Tuple[Optional[str], Dict[int, int]]:
        """Returns (latest_session_id, {1: last_s1_idx, 2: last_s2_idx})."""
        with self._lock, self._connection() as conn:
            cursor = conn.execute("""
            SELECT session_id, ended FROM sessions ORDER BY rowid DESC LIMIT 1;
            """)
            row = cursor.fetchone()
            if not row:
                return None, {1: -1, 2: -1}
            session_id = row["session_id"]
            is_ended = bool(row["ended"])
            if is_ended:
                return None, {1: -1, 2: -1}
                
            cursors = {1: -1, 2: -1}
            cur_cursor = conn.execute("""
            SELECT sensor, last_index FROM reader_state WHERE session_id = ?;
            """, (session_id,))
            for r in cur_cursor.fetchall():
                cursors[r["sensor"]] = r["last_index"]
            return session_id, cursors

    def get_reader_cursor(self, session_id: str, sensor: int) -> int:
        with self._lock, self._connection() as conn:
            cursor = conn.execute("""
            SELECT last_index FROM reader_state 
            WHERE session_id = ? AND sensor = ?;
            """, (session_id, sensor))
            row = cursor.fetchone()
            return row["last_index"] if row and row["last_index"] is not None else -1

    def set_reader_cursor(self, session_id: str, sensor: int, last_index: int):
        with self._lock, self._connection() as conn:
            conn.execute("""
            INSERT INTO reader_state (session_id, sensor, last_index, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(session_id, sensor) DO UPDATE SET 
                last_index = MAX(last_index, excluded.last_index),
                updated_at = excluded.updated_at;
            """, (session_id, sensor, last_index, time.time()))
            conn.commit()

    # --- Sample & Lost Data Persistence ---

    def save_samples_batch(
        self,
        session_id: str,
        sensor: int,
        first_index: int,
        data: List[List[int]]
    ) -> int:
        """
        Store a batch of samples atomically and update reader cursor on disk.
        data is a list of [val1, val2, val3] for sensor 1, or [val1, val2] for sensor 2.
        Returns the number of samples inserted.
        """
        if not data:
            return 0
            
        rows = []
        last_idx = first_index + len(data) - 1
        for offset, vals in enumerate(data):
            idx = first_index + offset
            v1 = vals[0] if len(vals) > 0 else 0
            v2 = vals[1] if len(vals) > 1 else 0
            v3 = vals[2] if len(vals) > 2 else 0
            rows.append((session_id, sensor, idx, v1, v2, v3))
            
        with self._lock, self._connection() as conn:
            try:
                cursor = conn.executemany("""
                INSERT OR IGNORE INTO samples (session_id, sensor, sample_index, val1, val2, val3, synced)
                VALUES (?, ?, ?, ?, ?, ?, 0);
                """, rows)
                
                # Atomically update reader cursor on disk
                conn.execute("""
                INSERT INTO reader_state (session_id, sensor, last_index, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(session_id, sensor) DO UPDATE SET 
                    last_index = MAX(last_index, excluded.last_index),
                    updated_at = excluded.updated_at;
                """, (session_id, sensor, last_idx, time.time()))
                
                conn.commit()
                return cursor.rowcount
            except sqlite3.OperationalError as e:
                if "full" in str(e).lower():
                    # Disk is full: release emergency reserve and record loss
                    self.release_reserve()
                    self.record_lost(session_id, sensor, first_index, last_idx, "disk_full")
                    return 0
                raise

    def record_lost(
        self,
        session_id: str,
        sensor: int,
        from_index: int,
        to_index: Optional[int],
        reason: str
    ):
        """Record an explicit data loss interval atomically and update cursor if to_index is set."""
        with self._lock, self._connection() as conn:
            try:
                conn.execute("""
                INSERT OR IGNORE INTO lost_ranges (session_id, sensor, from_index, to_index, reason, synced)
                VALUES (?, ?, ?, ?, ?, 0);
                """, (session_id, sensor, from_index, to_index, reason))
                
                if to_index is not None:
                    conn.execute("""
                    INSERT INTO reader_state (session_id, sensor, last_index, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(session_id, sensor) DO UPDATE SET 
                        last_index = MAX(last_index, excluded.last_index),
                        updated_at = excluded.updated_at;
                    """, (session_id, sensor, to_index, time.time()))
                    
                conn.commit()
            except sqlite3.OperationalError as e:
                if "full" in str(e).lower():
                    self.release_reserve()
                    # Retry once after freeing reserve
                    try:
                        conn.execute("""
                        INSERT OR IGNORE INTO lost_ranges (session_id, sensor, from_index, to_index, reason, synced)
                        VALUES (?, ?, ?, ?, ?, 0);
                        """, (session_id, sensor, from_index, to_index, reason))
                        conn.commit()
                    except Exception:
                        pass
                else:
                    raise

    def get_max_sample_index(self, session_id: str, sensor: int) -> int:
        """Get the highest received sample index for this sensor (-1 if none)."""
        with self._lock, self._connection() as conn:
            cursor = conn.execute("""
            SELECT MAX(sample_index) as max_idx FROM samples
            WHERE session_id = ? AND sensor = ?;
            """, (session_id, sensor))
            row = cursor.fetchone()
            if row and row["max_idx"] is not None:
                return row["max_idx"]
            return -1

    def get_unsynced_sessions(self) -> List[str]:
        """Returns all sessions that have pending samples, lost records, or results to sync."""
        with self._lock, self._connection() as conn:
            cursor = conn.execute("""
            SELECT DISTINCT session_id FROM (
                SELECT session_id FROM samples WHERE synced = 0
                UNION
                SELECT session_id FROM lost_ranges WHERE synced = 0
                UNION
                SELECT session_id FROM results WHERE synced = 0
            );
            """)
            return [r["session_id"] for r in cursor.fetchall()]

    def get_unsynced_samples(
        self,
        session_id: str,
        limit: int = 10000
    ) -> List[Dict[str, Any]]:
        """Retrieve a batch of unsynced samples for upload to cloud."""
        with self._lock, self._connection() as conn:
            cursor = conn.execute("""
            SELECT sensor, sample_index, val1, val2, val3
            FROM samples
            WHERE session_id = ? AND synced = 0
            ORDER BY sample_index ASC, sensor ASC
            LIMIT ?;
            """, (session_id, limit))
            results = []
            for r in cursor.fetchall():
                sensor = r["sensor"]
                vals = [r["val1"], r["val2"], r["val3"]] if sensor == 1 else [r["val1"], r["val2"]]
                results.append({
                    "sensor": sensor,
                    "index": r["sample_index"],
                    "values": vals
                })
            return results

    def mark_samples_synced_range(
        self,
        session_id: str,
        sensor: int,
        start_idx: int,
        end_idx: int
    ):
        """Mark range of samples as acknowledged by the cloud in one fast query."""
        with self._lock, self._connection() as conn:
            conn.execute("""
            UPDATE samples SET synced = 1
            WHERE session_id = ? AND sensor = ? AND sample_index BETWEEN ? AND ?;
            """, (session_id, sensor, start_idx, end_idx))
            conn.commit()

    def mark_samples_synced(
        self,
        session_id: str,
        sample_keys: List[Tuple[int, int]]
    ):
        """Mark discrete sample keys (sensor, sample_index) as acknowledged."""
        if not sample_keys:
            return
        with self._lock, self._connection() as conn:
            conn.executemany("""
            UPDATE samples SET synced = 1
            WHERE session_id = ? AND sensor = ? AND sample_index = ?;
            """, [(session_id, s, idx) for s, idx in sample_keys])
            conn.commit()

    def get_unsynced_lost(self, session_id: str) -> List[Dict[str, Any]]:
        with self._lock, self._connection() as conn:
            cursor = conn.execute("""
            SELECT id, sensor, from_index, to_index, reason
            FROM lost_ranges
            WHERE session_id = ? AND synced = 0
            ORDER BY id ASC;
            """, (session_id,))
            return [dict(r) for r in cursor.fetchall()]

    def mark_lost_synced(self, ids: List[int]):
        if not ids:
            return
        with self._lock, self._connection() as conn:
            conn.executemany("""
            UPDATE lost_ranges SET synced = 1 WHERE id = ?;
            """, [(i,) for i in ids])
            conn.commit()

    def get_unsynced_results(self, session_id: str) -> List[Dict[str, Any]]:
        with self._lock, self._connection() as conn:
            cursor = conn.execute("""
            SELECT window, mode, result_json FROM results
            WHERE session_id = ? AND synced = 0;
            """, (session_id,))
            out = []
            for r in cursor.fetchall():
                d = json.loads(r["result_json"])
                out.append(d)
            return out

    def mark_results_synced(self, session_id: str, window_modes: List[Tuple[int, str]]):
        if not window_modes:
            return
        with self._lock, self._connection() as conn:
            conn.executemany("""
            UPDATE results SET synced = 1
            WHERE session_id = ? AND window = ? AND mode = ?;
            """, [(session_id, w, m) for w, m in window_modes])
            conn.commit()

    def get_stats(self, session_id: Optional[str] = None) -> Dict[str, int]:
        with self._lock, self._connection() as conn:
            if session_id:
                cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN synced = 1 THEN 1 ELSE 0 END) as synced_count
                FROM samples WHERE session_id = ?;
                """, (session_id,))
            else:
                cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN synced = 1 THEN 1 ELSE 0 END) as synced_count
                FROM samples;
                """)
            row = cursor.fetchone()
            total = row["total"] if row and row["total"] is not None else 0
            synced = row["synced_count"] if row and row["synced_count"] is not None else 0
            return {
                "samples_saved": total,
                "samples_cloud": synced,
                "backlog": total - synced
            }

    # --- Window Results & Processing Support ---

    def get_samples_for_window(
        self,
        session_id: str,
        window: int
    ) -> Tuple[Dict[int, List[int]], Dict[int, List[int]]]:
        """
        Fetch samples for window w (samples 400w .. 400w + 399).
        Also includes sample 400w + 400 for sensor 1 to support peak neighbor checking.
        """
        start_idx = 400 * window
        end_idx = 400 * window + 399
        with self._lock, self._connection() as conn:
            cursor = conn.execute("""
            SELECT sensor, sample_index, val1, val2, val3
            FROM samples
            WHERE session_id = ? AND sample_index BETWEEN ? AND ?;
            """, (session_id, start_idx, end_idx + 1))
            
            s1: Dict[int, List[int]] = {}
            s2: Dict[int, List[int]] = {}
            for r in cursor.fetchall():
                if r["sensor"] == 1:
                    s1[r["sample_index"]] = [r["val1"], r["val2"], r["val3"]]
                elif r["sensor"] == 2 and r["sample_index"] <= end_idx:
                    s2[r["sample_index"]] = [r["val1"], r["val2"]]
            return s1, s2

    def get_s1_lookback_samples(
        self,
        session_id: str,
        window: int
    ) -> Dict[int, List[int]]:
        """Fetch sensor 1 samples in [400w - 800, 400w + 400] for peak detection."""
        start_idx = max(0, 400 * window - 800)
        end_idx = 400 * window + 400
        with self._lock, self._connection() as conn:
            cursor = conn.execute("""
            SELECT sample_index, val1, val2, val3
            FROM samples
            WHERE session_id = ? AND sensor = 1 AND sample_index BETWEEN ? AND ?;
            """, (session_id, start_idx, end_idx))
            return {r["sample_index"]: [r["val1"], r["val2"], r["val3"]] for r in cursor.fetchall()}

    def get_s1_c2_history(
        self,
        session_id: str,
        end_index: int,
        lookback_samples: int = 2400
    ) -> Dict[int, int]:
        """Fetch sensor 1 channel 2 history up to end_index (bounded lookback for high performance)."""
        start_index = max(0, end_index - lookback_samples)
        with self._lock, self._connection() as conn:
            cursor = conn.execute("""
            SELECT sample_index, val2
            FROM samples
            WHERE session_id = ? AND sensor = 1 AND sample_index BETWEEN ? AND ?;
            """, (session_id, start_index, end_index))
            return {r["sample_index"]: r["val2"] for r in cursor.fetchall()}

    def save_result(self, session_id: str, window: int, result_dict: Dict[str, Any], mode: str = "offline"):
        processed_at = result_dict.get("processed_at", time.time())
        result_dict["processed_at"] = processed_at
        with self._lock, self._connection() as conn:
            conn.execute("""
            INSERT OR REPLACE INTO results (session_id, window, mode, result_json, processed_at, synced)
            VALUES (?, ?, ?, ?, ?, 0);
            """, (session_id, window, mode, json.dumps(result_dict), processed_at))
            conn.commit()

    def get_results(self, session_id: str) -> List[Dict[str, Any]]:
        """Retrieve results sorted by window ASC, then mode (offline before online)."""
        with self._lock, self._connection() as conn:
            cursor = conn.execute("""
            SELECT result_json FROM results
            WHERE session_id = ?
            ORDER BY window ASC, CASE mode WHEN 'offline' THEN 1 ELSE 2 END ASC;
            """, (session_id,))
            return [json.loads(r["result_json"]) for r in cursor.fetchall()]

    # --- KV Metadata Store ---

    def set_kv(self, key: str, val: str):
        with self._lock, self._connection() as conn:
            conn.execute("""
            INSERT INTO kv (key, val, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET val = excluded.val, updated_at = excluded.updated_at;
            """, (key, val, time.time()))
            conn.commit()

    def get_kv(self, key: str) -> Optional[str]:
        with self._lock, self._connection() as conn:
            cursor = conn.execute("SELECT val FROM kv WHERE key = ?;", (key,))
            row = cursor.fetchone()
            return row["val"] if row else None

    # --- Export ---

    def export_data_generator(self, session_id: str) -> Generator[str, None, None]:
        """
        Required command format:
        pipeline export --session ID --from device
        Data as JSON lines, one per sample:
        {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}
        Then one line per loss:
        {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "..."}
        """
        with self._connection() as conn:
            cursor = conn.execute("""
            SELECT sensor, sample_index, val1, val2, val3
            FROM samples
            WHERE session_id = ?
            ORDER BY sample_index ASC, sensor ASC;
            """, (session_id,))
            for r in cursor:
                sensor = r["sensor"]
                vals = [r["val1"], r["val2"], r["val3"]] if sensor == 1 else [r["val1"], r["val2"]]
                line = json.dumps({
                    "sensor": sensor,
                    "index": r["sample_index"],
                    "values": vals
                }, separators=(',', ':'))
                yield line

            lost_cursor = conn.execute("""
            SELECT sensor, from_index, to_index, reason
            FROM lost_ranges
            WHERE session_id = ?
            ORDER BY id ASC;
            """, (session_id,))
            for r in lost_cursor:
                line = json.dumps({
                    "sensor": r["sensor"],
                    "from_index": r["from_index"],
                    "to_index": r["to_index"],
                    "reason": r["reason"]
                }, separators=(',', ':'))
                yield line
