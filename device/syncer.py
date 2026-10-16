"""
Device cloud synchronizer, network monitor, and offline window processing engine.
Implements:
- Continuous health probing with monotonic clock (immune to clock changes)
- Seamless online <-> offline mode switching without data loss or duplication (< 10s switch)
- Real-time cloud streaming in online mode (with gzip compression for high throughput)
- Local window processing in offline mode using O(1) incremental peak detection
- High-throughput catch-up streaming (catches up 2 hours of data in < 15 minutes)
- Multi-session backlog synchronization
"""

import gzip
import json
import time
import urllib.request
import urllib.error
from typing import Dict, List, Optional, Any, Tuple
from core.storage import DeviceStorage
from core.processing import compute_window

class DeviceSyncer:
    def __init__(
        self,
        storage: DeviceStorage,
        cloud_url: str = "http://127.0.0.1:8000",
        probe_interval: float = 1.0,
        probe_timeout: float = 1.5,
        batch_size: int = 10000
    ):
        self.storage = storage
        self.cloud_url = cloud_url.rstrip("/")
        self.probe_interval = probe_interval
        self.probe_timeout = probe_timeout
        self.batch_size = batch_size
        
        self.current_mode: str = "offline"  # starts offline until verified
        self.consecutive_failures: int = 0
        self.last_probe_time: float = 0.0  # measured with monotonic clock
        self.running: bool = False
        
        # Incremental processor state per session:
        # session_id -> {
        #   "next_window": int,
        #   "last_peak": Optional[int],
        #   "recent_peaks": List[int],
        #   "recent_missing": List[int]
        # }
        self._proc_state: Dict[str, Dict[str, Any]] = {}

    def probe_cloud_health(self) -> bool:
        """Probe cloud /health endpoint. Fast timeout to guarantee switch within 10s."""
        url = f"{self.cloud_url}/health"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "MayaCardio-Device"})
            with urllib.request.urlopen(req, timeout=self.probe_timeout) as resp:
                if resp.status == 200:
                    return True
                return False
        except Exception:
            return False

    def check_and_update_mode(self):
        """
        Move to offline mode when the network is lost, and back to online mode when
        it returns, each within 10 seconds.
        Uses time.monotonic() so changes to the system clock cannot stall probing.
        """
        now = time.monotonic()
        if now - self.last_probe_time < self.probe_interval:
            return
            
        self.last_probe_time = now
        healthy = self.probe_cloud_health()
        
        if healthy:
            self.consecutive_failures = 0
            if self.current_mode != "online":
                self.current_mode = "online"
                self.storage.set_kv("mode", "online")
        else:
            self.consecutive_failures += 1
            if self.consecutive_failures >= 2 and self.current_mode != "offline":
                self.current_mode = "offline"
                self.storage.set_kv("mode", "offline")

    def push_batch_to_cloud(self, session_id: str) -> bool:
        """
        Send a batch of unsynced samples, lost ranges, and offline results to the cloud.
        Payload is gzip-compressed for maximum throughput during catch-up.
        """
        unsynced_samples = self.storage.get_unsynced_samples(session_id, limit=self.batch_size)
        unsynced_lost = self.storage.get_unsynced_lost(session_id)
        unsynced_results = self.storage.get_unsynced_results(session_id)
        
        if not unsynced_samples and not unsynced_lost and not unsynced_results:
            return False
            
        payload = {
            "session_id": session_id,
            "samples": unsynced_samples,
            "lost": unsynced_lost,
            "results": unsynced_results
        }
        
        url = f"{self.cloud_url}/api/ingest"
        raw_json = json.dumps(payload).encode("utf-8")
        compressed_data = gzip.compress(raw_json)
        
        req = urllib.request.Request(
            url,
            data=compressed_data,
            headers={
                "Content-Type": "application/json",
                "Content-Encoding": "gzip"
            }
        )
        try:
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                if resp.status == 200:
                    self.consecutive_failures = 0
                    if self.current_mode != "online":
                        self.current_mode = "online"
                        self.storage.set_kv("mode", "online")
                        
                    # Acknowledge samples
                    keys_to_ack = [(s["sensor"], s["index"]) for s in unsynced_samples]
                    self.storage.mark_samples_synced(session_id, keys_to_ack)
                    
                    # Acknowledge lost records
                    lost_ids_to_ack = [item["id"] for item in unsynced_lost]
                    self.storage.mark_lost_synced(lost_ids_to_ack)
                    
                    # Acknowledge offline results uploaded
                    res_keys = [(r["window"], r.get("mode", "offline")) for r in unsynced_results]
                    self.storage.mark_results_synced(session_id, res_keys)
                    return True
                else:
                    self.consecutive_failures += 1
                    if self.consecutive_failures >= 2:
                        self.current_mode = "offline"
                        self.storage.set_kv("mode", "offline")
                    return False
        except Exception:
            self.consecutive_failures += 1
            if self.consecutive_failures >= 2:
                self.current_mode = "offline"
                self.storage.set_kv("mode", "offline")
            return False

    def process_offline_windows(self, session_id: str):
        """
        When in offline mode, process ready 1-second windows locally.
        Window w holds samples 400w to 400w + 399.
        Crucial requirement: Window w is only processed once both sensors have
        reached at least index 400w + 399 (and 400w + 400 for sensor 1 peak check),
        preventing premature calculation of incomplete windows!
        """
        s1_max = self.storage.get_max_sample_index(session_id, 1)
        s2_max = self.storage.get_max_sample_index(session_id, 2)
        if s1_max < 0 or s2_max < 0:
            return
            
        # A window w needs sensor 1 up to 400w + 400 (for +1 peak neighbor) and sensor 2 up to 400w + 399
        max_ready_w1 = (s1_max - 400) // 400 if s1_max >= 400 else -1
        max_ready_w2 = (s2_max + 1) // 400 - 1
        max_ready_window = min(max_ready_w1, max_ready_w2)
        if max_ready_window < 0:
            return
            
        # Initialize or retrieve incremental state
        if session_id not in self._proc_state:
            existing = self.storage.get_results(session_id)
            existing_offline_windows = {r["window"] for r in existing if r.get("mode") == "offline"}
            next_w = max(existing_offline_windows) + 1 if existing_offline_windows else 0
            self._proc_state[session_id] = {
                "next_window": next_w,
                "last_peak": None,
                "recent_peaks": [],
                "recent_missing": []
            }
            
        st = self._proc_state[session_id]
        
        while st["next_window"] <= max_ready_window:
            w = st["next_window"]
            # Fetch window samples
            s1_samples = self.storage.get_s1_lookback_samples(session_id, w)
            _, s2_samples = self.storage.get_samples_for_window(session_id, w)
            
            res, last_p, r_peaks, r_miss = compute_window(
                window=w,
                session_id=session_id,
                s1=s1_samples,
                s2=s2_samples,
                last_peak=st["last_peak"],
                recent_peaks=st["recent_peaks"],
                recent_missing=st["recent_missing"],
                mode="offline"
            )
            
            self.storage.save_result(session_id, w, res, mode="offline")
            
            st["next_window"] = w + 1
            st["last_peak"] = last_p
            st["recent_peaks"] = r_peaks
            st["recent_missing"] = r_miss

    def step_sync(self):
        """Single synchronization and state update step."""
        self.check_and_update_mode()
        
        # Check all sessions that have pending sync data
        unsynced_sessions = self.storage.get_unsynced_sessions()
        active_session = self.storage.get_active_session()
        
        sessions_to_check = list(unsynced_sessions)
        if active_session and active_session not in sessions_to_check:
            sessions_to_check.append(active_session)
            
        if not sessions_to_check:
            time.sleep(0.05)
            return
            
        if self.current_mode == "online":
            pushed_any = False
            for sid in sessions_to_check:
                pushed = self.push_batch_to_cloud(sid)
                if pushed:
                    pushed_any = True
            if not pushed_any:
                time.sleep(0.02)
        else:
            # Offline mode: process ready windows locally
            if active_session:
                self.process_offline_windows(active_session)
            time.sleep(0.05)

    def run_loop(self):
        self.running = True
        self.storage.set_kv("mode", self.current_mode)
        while self.running:
            try:
                self.step_sync()
            except Exception:
                time.sleep(0.05)
