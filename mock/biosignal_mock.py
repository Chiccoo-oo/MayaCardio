"""
Faithful implementation of biosignal-mock per Section 6 specifications.
Runs on board and imitates the bedside device's two sensors.
Supports both --mode signal and --mode ramp, seeding, fault injection,
10-second ring buffer, and CRC32 verification.
"""

import argparse
import json
import math
import random
import socket
import sys
import threading
import time
import zlib
from typing import Dict, List, Optional, Tuple, Any

SAMPLE_RATE = 400  # samples per second
BUFFER_SECONDS = 10
BUFFER_SAMPLES = SAMPLE_RATE * BUFFER_SECONDS  # 4000 samples

def compute_ramp_sample(sensor: int, sample_idx: int) -> List[int]:
    """
    Sensor 1 channel c at sample i = (3i + c) mod 8,388,608
    Sensor 2 channel c at sample i = (2i + c) mod 8,388,608
    """
    mod_val = 8388608
    if sensor == 1:
        return [(3 * sample_idx + c) % mod_val for c in range(3)]
    else:
        return [(2 * sample_idx + c) % mod_val for c in range(2)]

class BiosignalGenerator:
    def __init__(self, mode: str, seed: int, duration_sec: int):
        self.mode = mode
        self.seed = seed
        self.duration_sec = duration_sec
        self.total_samples = duration_sec * SAMPLE_RATE
        self.rng = random.Random(seed)
        
        # Precompute signal baseline offsets if mode == 'signal'
        self.s2_c1_phase = self.rng.random() * 2 * math.pi
        self.s2_c2_phase = self.rng.random() * 2 * math.pi

    def get_sample(self, sensor: int, sample_idx: int) -> List[int]:
        if self.mode == "ramp":
            return compute_ramp_sample(sensor, sample_idx)
        else:
            # Deterministic biosignal approximation
            t = sample_idx / SAMPLE_RATE
            if sensor == 1:
                # 3 channels: ECG simulation with R-peaks
                # HR around 75 bpm -> cycle ~0.8s (320 samples)
                cycle_pos = (sample_idx % 320) / 320.0
                # Channel 2 is lead II: sharp peak at cycle_pos ~ 0.2
                if 0.19 <= cycle_pos <= 0.21:
                    peak_val = int(1200 + 400 * math.sin((cycle_pos - 0.2) / 0.01 * math.pi / 2))
                else:
                    peak_val = int(200 + 50 * math.sin(2 * math.pi * t))
                    
                ch1 = int(300 + 100 * math.sin(2 * math.pi * t * 1.2))
                ch2 = peak_val
                ch3 = int(250 + 80 * math.cos(2 * math.pi * t * 0.8))
                return [ch1, ch2, ch3]
            else:
                # 2 channels: PPG / Accelerometer
                ch1 = int(500 + 200 * math.sin(2 * math.pi * t + self.s2_c1_phase))
                ch2 = int(450 + 150 * math.cos(2 * math.pi * t + self.s2_c2_phase))
                return [ch1, ch2]

class MockServer:
    def __init__(
        self,
        mode: str = "signal",
        seed: int = 7,
        duration: int = 3600,
        port: int = 7100,
        session_id: str = "5f0c2a9e",
        fault_flags: Optional[Dict[str, Any]] = None
    ):
        self.mode = mode
        self.seed = seed
        self.duration = duration
        self.port = port
        self.session_id = session_id
        self.fault_flags = fault_flags or {}
        
        self.generator = BiosignalGenerator(mode, seed, duration)
        self.start_time = 0.0
        self.running = False
        self.sock: Optional[socket.socket] = None
        self.server_thread: Optional[threading.Thread] = None
        
        # Track client read cursors per sensor: sensor -> last sample index read
        self.client_read_cursors: Dict[int, int] = {1: -1, 2: -1}

    def start(self, non_blocking: bool = False):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", self.port))
        self.sock.listen(5)
        self.start_time = time.time()
        self.running = True
        
        print(f"[biosignal-mock] Listening on port {self.port} (mode={self.mode}, seed={self.seed}, session={self.session_id})")
        
        if non_blocking:
            self.server_thread = threading.Thread(target=self._accept_loop, daemon=True)
            self.server_thread.start()
        else:
            self._accept_loop()

    def restart(self, new_session_id: str):
        """Simulate a mock server restart (Section 6.3)."""
        self.session_id = new_session_id
        self.start_time = time.time()
        self.client_read_cursors = {1: -1, 2: -1}

    def stop(self):
        self.running = False
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass

    def _accept_loop(self):
        while self.running:
            try:
                conn, addr = self.sock.accept()
                self._handle_client(conn, addr)
            except Exception:
                break

    def _handle_client(self, conn: socket.socket, addr):
        conn_file = conn.makefile("r", encoding="utf-8")
        try:
            for line in conn_file:
                if not self.running:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    req = json.loads(line)
                except Exception:
                    continue
                if req.get("cmd") == "read":
                    # Fault injection: stall before replying
                    stall = self.fault_flags.get("stall_seconds", 0)
                    if stall > 0:
                        time.sleep(stall)
                        self.fault_flags["stall_seconds"] = 0
                        
                    resp = self._process_read(req)
                    if resp is not None:
                        conn.sendall((resp + "\n").encode("utf-8"))
                        # Fault injection: duplicate reply (sent twice!)
                        if self.fault_flags.get("duplicate_reply"):
                            conn.sendall((resp + "\n").encode("utf-8"))
                            self.fault_flags["duplicate_reply"] = False
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _process_read(self, req: Dict[str, Any]) -> Optional[str]:
        sensor = req.get("sensor", 1)
        requested_from = req.get("from_index")
        
        # Check sensor drop fault
        if self.fault_flags.get("sensor_drop") == sensor:
            return json.dumps({
                "session_id": self.session_id,
                "sensor": sensor,
                "first_index": requested_from or (self.client_read_cursors[sensor] + 1),
                "count": 0,
                "lost": [{"from_index": requested_from or (self.client_read_cursors[sensor] + 1), "to_index": None, "reason": "sensor_dropout"}],
                "session_end": False,
                "crc32": "00000000",
                "data": []
            }, separators=(',', ':'))

        now = time.time()
        elapsed = now - self.start_time
        current_produced_idx = int(elapsed * SAMPLE_RATE)
        max_limit = self.generator.total_samples - 1
        current_produced_idx = min(current_produced_idx, max_limit)
        
        # 10 second buffer limit
        oldest_kept_idx = max(0, current_produced_idx - BUFFER_SAMPLES + 1)
        
        last_cursor = self.client_read_cursors[sensor]
        lost_ranges: List[Dict[str, Any]] = []
        
        if requested_from is not None:
            # Re-read requested
            first_idx = requested_from
        else:
            # Next data requested
            first_idx = last_cursor + 1
            
        # Check if first_idx is older than buffer
        if first_idx < oldest_kept_idx:
            # Buffer overflow loss!
            lost_ranges.append({
                "from_index": first_idx,
                "to_index": oldest_kept_idx - 1,
                "reason": "buffer_overflow"
            })
            first_idx = oldest_kept_idx
            
        # How many samples can we deliver? (at most 1 second of data = 400 samples)
        available = max(0, current_produced_idx - first_idx + 1)
        count = min(SAMPLE_RATE, available)
        
        if count <= 0:
            count = 0
            samples_data = []
        else:
            samples_data = [self.generator.get_sample(sensor, first_idx + i) for i in range(count)]
            self.client_read_cursors[sensor] = first_idx + count - 1

        session_end = (first_idx + count >= self.generator.total_samples)
        
        # Format data array text for CRC32
        data_text = json.dumps(samples_data, separators=(',', ':'))
        crc_val = zlib.crc32(data_text.encode('utf-8')) & 0xffffffff
        crc_hex = f"{crc_val:08x}"
        
        # Check fault injection flags if enabled
        if self.fault_flags.get("corrupt_crc"):
            crc_hex = "00000000"
            self.fault_flags["corrupt_crc"] = False  # inject once
            
        reply_dict = {
            "session_id": self.session_id,
            "sensor": sensor,
            "first_index": first_idx,
            "count": count,
            "lost": lost_ranges,
            "session_end": session_end,
            "crc32": crc_hex,
            "data": samples_data
        }
        
        return json.dumps(reply_dict, separators=(',', ':'))

def main():
    parser = argparse.ArgumentParser(description="biosignal-mock reference service")
    parser.add_argument("--mode", choices=["signal", "ramp"], default="signal", help="signal or ramp")
    parser.add_argument("--seed", type=int, default=7, help="random seed")
    parser.add_argument("--duration", type=int, default=3600, help="duration in seconds")
    parser.add_argument("--port", type=int, default=7100, help="TCP port")
    parser.add_argument("--session", type=str, default="5f0c2a9e", help="Session ID")
    args = parser.parse_args()
    
    server = MockServer(
        mode=args.mode,
        seed=args.seed,
        duration=args.duration,
        port=args.port,
        session_id=args.session
    )
    server.start(non_blocking=False)

if __name__ == "__main__":
    main()
