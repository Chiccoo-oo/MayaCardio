"""
Robust device collector and reader for biosignal-mock TCP stream.
Handles:
- TCP protocol, newline JSON, reconnects
- CRC-32 (zlib) verification with automatic re-read on corruption
- Reply deduplication and handling of retransmitted/out-of-order replies (Section 6.3)
- Session transition detection and unread data lost tracking ("to_index": null)
- Mock reported lost intervals (buffer overflow, sensor dropout, etc.)
- Atomic SQLite persistence with WAL durability and persisted cursors
"""

import json
import socket
import time
import zlib
from typing import Dict, List, Optional, Any, Tuple
from core.storage import DeviceStorage

class DeviceReader:
    def __init__(
        self,
        storage: DeviceStorage,
        mock_host: str = "127.0.0.1",
        mock_port: int = 7100,
        read_timeout: float = 2.0
    ):
        self.storage = storage
        self.mock_host = mock_host
        self.mock_port = mock_port
        self.read_timeout = read_timeout
        
        self.running = False
        self.sock: Optional[socket.socket] = None
        self.recv_buf = b""
        
        # Resume state from SQLite storage
        session_id, cursors = self.storage.get_latest_session_info()
        self.current_session_id: Optional[str] = session_id
        self.last_received_idx: Dict[int, int] = cursors
        if session_id:
            for s in (1, 2):
                db_cursor = self.storage.get_reader_cursor(session_id, s)
                if db_cursor > self.last_received_idx[s]:
                    self.last_received_idx[s] = db_cursor

    def connect(self) -> bool:
        self.close_socket()
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(self.read_timeout)
            self.sock.connect((self.mock_host, self.mock_port))
            self.recv_buf = b""
            return True
        except Exception:
            self.sock = None
            self.recv_buf = b""
            return False

    def close_socket(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
        self.recv_buf = b""

    def close(self):
        self.running = False
        self.close_socket()

    def _drain_extra_input(self):
        """Drain any unconsumed bytes or stale lines on the socket without blocking."""
        if not self.sock:
            return
        try:
            self.sock.setblocking(False)
            while True:
                chunk = self.sock.recv(4096)
                if not chunk:
                    break
                self.recv_buf += chunk
        except (BlockingIOError, socket.error):
            pass
        finally:
            if self.sock:
                self.sock.settimeout(self.read_timeout)

    def _readline_buffered(self) -> Optional[str]:
        """Read a single newline-terminated line from the socket using our buffer."""
        deadline = time.monotonic() + self.read_timeout
        while b"\n" not in self.recv_buf:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            try:
                self.sock.settimeout(max(0.1, remaining))
                chunk = self.sock.recv(4096)
                if not chunk:
                    return None
                self.recv_buf += chunk
            except socket.timeout:
                return None
            except Exception:
                return None

        line_bytes, self.recv_buf = self.recv_buf.split(b"\n", 1)
        return line_bytes.decode("utf-8", errors="replace").strip()

    def _verify_crc(self, raw_line: str, expected_crc_hex: str) -> bool:
        """
        Extract the `data` array text exactly as sent, from [ to the matching ],
        and compute CRC-32 (zlib).
        """
        data_key_idx = raw_line.find('"data":')
        if data_key_idx == -1:
            return False
            
        bracket_start = raw_line.find('[', data_key_idx)
        if bracket_start == -1:
            return False
            
        bracket_count = 0
        bracket_end = -1
        in_string = False
        escape = False
        for i in range(bracket_start, len(raw_line)):
            ch = raw_line[i]
            if escape:
                escape = False
                continue
            if ch == '\\':
                escape = True
                continue
            if ch == '"':
                in_string = not in_string
                continue
            if not in_string:
                if ch == '[':
                    bracket_count += 1
                elif ch == ']':
                    bracket_count -= 1
                    if bracket_count == 0:
                        bracket_end = i
                        break
                        
        if bracket_end == -1:
            return False
            
        data_text = raw_line[bracket_start:bracket_end + 1]
        calc_crc = zlib.crc32(data_text.encode('utf-8')) & 0xffffffff
        calc_hex = f"{calc_crc:08x}"
        return calc_hex.lower() == expected_crc_hex.lower()

    def step_read(self, sensor: int) -> bool:
        """
        Request next data for given sensor.
        Guarantees:
        - Out-of-sync / duplicate replies are associated with actual reply["sensor"], not requested sensor.
        - CRC-32 validated before data or lost ranges are accepted.
        - Session transitions detect and record unread data of prior session with to_index=null.
        """
        if not self.sock:
            if not self.connect():
                return False

        # First, process any complete lines that may already be in recv_buf (e.g. duplicate replies)
        while b"\n" in self.recv_buf:
            pending_line = self._readline_buffered()
            if pending_line:
                self._handle_reply(pending_line, fallback_sensor=sensor)

        last_idx = self.last_received_idx[sensor]
        if last_idx >= 0:
            req = {"cmd": "read", "sensor": sensor, "from_index": last_idx + 1}
        else:
            req = {"cmd": "read", "sensor": sensor}
            
        req_line = json.dumps(req) + "\n"
        try:
            self.sock.sendall(req_line.encode("utf-8"))
            raw_reply = self._readline_buffered()
            if raw_reply is None:
                # Timeout or socket error - reconnect on next attempt
                self.close_socket()
                return False
        except Exception:
            self.close_socket()
            return False
            
        return self._handle_reply(raw_reply, fallback_sensor=sensor)

    def _handle_reply(self, raw_reply_str: str, fallback_sensor: int) -> bool:
        if not raw_reply_str:
            return False
            
        try:
            reply = json.loads(raw_reply_str)
        except Exception:
            return False
            
        # 1. CRC32 Check
        expected_crc = reply.get("crc32", "")
        if not self._verify_crc(raw_reply_str, expected_crc):
            # Corrupted message! Reject completely.
            return False
            
        session_id = reply.get("session_id")
        if not session_id:
            return False

        # CRITICAL FIX (Section 6.3): Use reply["sensor"], NEVER assume requested sensor!
        reply_sensor = reply.get("sensor", fallback_sensor)
        
        # 2. Session transition detection
        if self.current_session_id is not None and self.current_session_id != session_id:
            old_session = self.current_session_id
            for s in (1, 2):
                old_last = self.last_received_idx[s]
                self.storage.record_lost(
                    session_id=old_session,
                    sensor=s,
                    from_index=old_last + 1 if old_last >= 0 else 0,
                    to_index=None,
                    reason="mock_restart"
                )
            self.storage.mark_session_ended(old_session)
            self.last_received_idx = {1: -1, 2: -1}
            
        self.current_session_id = session_id
        self.storage.register_session(session_id, time.time())
        
        # 3. Process mock-reported losses
        for lost_entry in reply.get("lost", []):
            self.storage.record_lost(
                session_id=session_id,
                sensor=reply_sensor,
                from_index=lost_entry["from_index"],
                to_index=lost_entry.get("to_index"),
                reason=lost_entry.get("reason", "unknown")
            )
            if lost_entry.get("to_index") is not None:
                if lost_entry["to_index"] > self.last_received_idx[reply_sensor]:
                    self.last_received_idx[reply_sensor] = lost_entry["to_index"]
                    self.storage.set_reader_cursor(session_id, reply_sensor, self.last_received_idx[reply_sensor])

        # 4. Save samples
        first_index = reply.get("first_index", 0)
        data = reply.get("data", [])
        if data:
            self.storage.save_samples_batch(
                session_id=session_id,
                sensor=reply_sensor,
                first_index=first_index,
                data=data
            )
            new_last = first_index + len(data) - 1
            if new_last > self.last_received_idx[reply_sensor]:
                self.last_received_idx[reply_sensor] = new_last
                self.storage.set_reader_cursor(session_id, reply_sensor, new_last)

        # 5. Handle session_end
        if reply.get("session_end"):
            # Check if both sensors are complete
            pass

        return True

    def run_loop(self):
        """Continuous collection loop alternating between sensors."""
        self.running = True
        while self.running:
            try:
                success1 = self.step_read(1)
                success2 = self.step_read(2)
                
                if not success1 and not success2:
                    time.sleep(0.05)
                else:
                    time.sleep(0.005)
            except Exception:
                time.sleep(0.05)
