"""
Cloud storage and online window processing engine.
Stores received samples, lost ranges, and offline results.
Runs online window processing within 5 seconds using incremental O(1) peak detection.
"""

import sqlite3
import json
import threading
import time
from contextlib import contextmanager
from typing import Dict, List, Optional, Any, Tuple, Generator
from core.processing import compute_window

class CloudStorage:
    def __init__(self, db_path: str = "maya_cloud.db"):
        self.db_path = db_path
        self._lock = threading.RLock()
        self._proc_state: Dict[str, Dict[str, Any]] = {}
        self._init_db()

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
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("PRAGMA busy_timeout = 60000;")
            
            conn.execute("""
            CREATE TABLE IF NOT EXISTS samples (
                session_id TEXT,
                sensor INTEGER,
                sample_index INTEGER,
                val1 INTEGER,
                val2 INTEGER,
                val3 INTEGER,
                received_at REAL,
                PRIMARY KEY (session_id, sensor, sample_index)
            ) WITHOUT ROWID;
            """)
            
            conn.execute("""
            CREATE TABLE IF NOT EXISTS lost_ranges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT,
                sensor INTEGER,
                from_index INTEGER,
                to_index INTEGER,
                reason TEXT
            );
            """)
            
            conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_cloud_lost_uniq
            ON lost_ranges (session_id, sensor, from_index, IFNULL(to_index, -1));
            """)
            
            conn.execute("""
            CREATE TABLE IF NOT EXISTS results (
                session_id TEXT,
                window INTEGER,
                mode TEXT,
                result_json TEXT,
                processed_at REAL,
                PRIMARY KEY (session_id, window, mode)
            ) WITHOUT ROWID;
            """)
            
            conn.commit()

    def ingest_samples(self, session_id: str, samples: List[Dict[str, Any]]) -> int:
        if not samples:
            return 0
            
        now = time.time()
        rows = []
        for s in samples:
            sensor = s["sensor"]
            idx = s["index"]
            vals = s["values"]
            v1 = vals[0] if len(vals) > 0 else 0
            v2 = vals[1] if len(vals) > 1 else 0
            v3 = vals[2] if len(vals) > 2 else 0
            rows.append((session_id, sensor, idx, v1, v2, v3, now))
            
        with self._lock, self._connection() as conn:
            cursor = conn.executemany("""
            INSERT OR IGNORE INTO samples (session_id, sensor, sample_index, val1, val2, val3, received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """, rows)
            conn.commit()
            return cursor.rowcount

    def ingest_lost(self, session_id: str, lost_items: List[Dict[str, Any]]):
        if not lost_items:
            return
        with self._lock, self._connection() as conn:
            for item in lost_items:
                conn.execute("""
                INSERT OR IGNORE INTO lost_ranges (session_id, sensor, from_index, to_index, reason)
                VALUES (?, ?, ?, ?, ?);
                """, (session_id, item["sensor"], item["from_index"], item.get("to_index"), item.get("reason", "unknown")))
            conn.commit()

    def ingest_results(self, session_id: str, results_list: List[Dict[str, Any]]):
        if not results_list:
            return
        with self._lock, self._connection() as conn:
            for r in results_list:
                w = r["window"]
                m = r.get("mode", "offline")
                p_at = r.get("processed_at", time.time())
                conn.execute("""
                INSERT OR REPLACE INTO results (session_id, window, mode, result_json, processed_at)
                VALUES (?, ?, ?, ?, ?);
                """, (session_id, w, m, json.dumps(r), p_at))
            conn.commit()

    def get_sample_count(self, session_id: Optional[str] = None) -> int:
        with self._lock, self._connection() as conn:
            if session_id:
                cursor = conn.execute("SELECT COUNT(*) as c FROM samples WHERE session_id = ?;", (session_id,))
            else:
                cursor = conn.execute("SELECT COUNT(*) as c FROM samples;")
            row = cursor.fetchone()
            return row["c"] if row else 0

    def get_max_sample_index(self, session_id: str, sensor: int) -> int:
        with self._lock, self._connection() as conn:
            cursor = conn.execute("""
            SELECT MAX(sample_index) as max_idx FROM samples
            WHERE session_id = ? AND sensor = ?;
            """, (session_id, sensor))
            row = cursor.fetchone()
            if row and row["max_idx"] is not None:
                return row["max_idx"]
            return -1

    def get_highest_online_window(self, session_id: str) -> int:
        with self._lock, self._connection() as conn:
            cursor = conn.execute("""
            SELECT MAX(window) as max_w FROM results WHERE session_id = ? AND mode = 'online';
            """, (session_id,))
            row = cursor.fetchone()
            if row and row["max_w"] is not None:
                return row["max_w"]
            return -1

    def process_pending_windows(self, session_id: str):
        """
        Check which windows have complete samples and process them online.
        Uses incremental compute_window to keep latency < 5 seconds and memory bounded.
        """
        s1_max = self.get_max_sample_index(session_id, 1)
        s2_max = self.get_max_sample_index(session_id, 2)
        if s1_max < 0 or s2_max < 0:
            return
            
        max_ready_w1 = (s1_max - 400) // 400 if s1_max >= 400 else -1
        max_ready_w2 = (s2_max + 1) // 400 - 1
        max_ready_window = min(max_ready_w1, max_ready_w2)
        if max_ready_window < 0:
            return

        if session_id not in self._proc_state:
            last_online = self.get_highest_online_window(session_id)
            self._proc_state[session_id] = {
                "next_window": last_online + 1,
                "last_peak": None,
                "recent_peaks": [],
                "recent_missing": []
            }
            
        st = self._proc_state[session_id]
        
        while st["next_window"] <= max_ready_window:
            w = st["next_window"]
            start_idx = 400 * w
            end_idx = 400 * w + 399
            
            # Fetch lookback samples for sensor 1 and window samples for sensor 2
            s1_start = max(0, start_idx - 800)
            s1_end = end_idx + 1  # includes 400w + 400
            
            with self._lock, self._connection() as conn:
                cursor = conn.execute("""
                SELECT sensor, sample_index, val1, val2, val3
                FROM samples
                WHERE session_id = ? AND sample_index BETWEEN ? AND ?;
                """, (session_id, s1_start, s1_end))
                
                s1: Dict[int, List[int]] = {}
                s2: Dict[int, List[int]] = {}
                for r in cursor.fetchall():
                    if r["sensor"] == 1:
                        s1[r["sample_index"]] = [r["val1"], r["val2"], r["val3"]]
                    elif r["sensor"] == 2 and start_idx <= r["sample_index"] <= end_idx:
                        s2[r["sample_index"]] = [r["val1"], r["val2"]]

            res, last_p, r_peaks, r_miss = compute_window(
                window=w,
                session_id=session_id,
                s1=s1,
                s2=s2,
                last_peak=st["last_peak"],
                recent_peaks=st["recent_peaks"],
                recent_missing=st["recent_missing"],
                mode="online"
            )
            
            processed_at = time.time()
            res["processed_at"] = processed_at
            
            with self._lock, self._connection() as conn:
                conn.execute("""
                INSERT OR REPLACE INTO results (session_id, window, mode, result_json, processed_at)
                VALUES (?, ?, 'online', ?, ?);
                """, (session_id, w, json.dumps(res), processed_at))
                conn.commit()

            st["next_window"] = w + 1
            st["last_peak"] = last_p
            st["recent_peaks"] = r_peaks
            st["recent_missing"] = r_miss

    def export_data_generator(self, session_id: str) -> Generator[str, None, None]:
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

    def get_results(self, session_id: str) -> List[Dict[str, Any]]:
        """Returns all session results sorted by window ASC, then mode."""
        with self._lock, self._connection() as conn:
            cursor = conn.execute("""
            SELECT result_json FROM results
            WHERE session_id = ?
            ORDER BY window ASC, CASE mode WHEN 'offline' THEN 1 ELSE 2 END ASC;
            """, (session_id,))
            return [json.loads(r["result_json"]) for r in cursor.fetchall()]
