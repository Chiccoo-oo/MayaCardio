"""
Automated Failure and Mode Switching Test Suite.
Validates:
1. Online -> Offline mode switch (< 10 seconds) upon network cut.
2. Device offline window processing continues seamlessly during network outage.
3. Offline -> Online mode switch (< 10 seconds) upon network restoration.
4. Catch-up streaming: cloud receives 100% of backlog with zero duplicates or loss.
5. Mock fault handling (Section 6.3):
   - CRC-32 corruption detection and recovery
   - Retransmitted / duplicate reply handling
   - Mock restart with "to_index": null loss tracking
   - Process crash & reboot cursor persistence recovery
"""

import json
import os
import shutil
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.storage import DeviceStorage
from core.processing import compute_window, compute_heart_rate, detect_peaks
from cloud.storage import CloudStorage
from cloud.server import CloudRequestHandler
from mock.biosignal_mock import MockServer, BiosignalGenerator
from mock.check_ground_truth import verify_export
from device.reader import DeviceReader
from device.syncer import DeviceSyncer
from http.server import ThreadingHTTPServer

class TestFailuresAndSwitching(unittest.TestCase):
    def setUp(self):
        self.device_db = "test_fail_device.db"
        self.cloud_db = "test_fail_cloud.db"
        for db in [self.device_db, self.cloud_db]:
            for ext in ["", "-wal", "-shm"]:
                p = db + ext
                if os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass
                        
        self.mock_port = 7201
        self.cloud_port = 8201

    def tearDown(self):
        for db in [self.device_db, self.cloud_db]:
            for ext in ["", "-wal", "-shm"]:
                p = db + ext
                if os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass

    def test_network_cut_offline_switch_and_catchup(self):
        """
        Tests criteria 4, 5, 6:
        - Online streaming
        - Network cut: switches to offline within < 10 seconds
        - Device continues local offline window processing
        - Network restored: switches to online within < 10 seconds
        - Cloud catch-up completed with zero silent loss or duplication
        """
        mock_server = MockServer(
            mode="ramp",
            seed=42,
            duration=60,
            port=self.mock_port,
            session_id="switch_session"
        )
        mock_server.start(non_blocking=True)
        
        cloud_storage = CloudStorage(self.cloud_db)
        CloudRequestHandler.storage = cloud_storage
        httpd = ThreadingHTTPServer(("127.0.0.1", self.cloud_port), CloudRequestHandler)
        t_cloud = threading.Thread(target=httpd.serve_forever, daemon=True)
        t_cloud.start()
        time.sleep(0.3)
        
        device_storage = DeviceStorage(self.device_db)
        reader = DeviceReader(storage=device_storage, mock_port=self.mock_port)
        syncer = DeviceSyncer(
            storage=device_storage,
            cloud_url=f"http://127.0.0.1:{self.cloud_port}",
            probe_interval=0.5,
            probe_timeout=0.8
        )
        
        t_r = threading.Thread(target=reader.run_loop, daemon=True)
        t_s = threading.Thread(target=syncer.run_loop, daemon=True)
        t_r.start()
        t_s.start()
        
        # Phase A: Online mode
        time.sleep(2.5)
        self.assertEqual(syncer.current_mode, "online", "Pipeline should be in ONLINE mode")
        
        # Phase B: Cut Network!
        cut_time = time.time()
        httpd.shutdown()
        httpd.server_close()
        
        while syncer.current_mode != "offline" and (time.time() - cut_time) < 10.0:
            time.sleep(0.1)
            
        switch_latency = time.time() - cut_time
        print(f"\n[Test] Network cut detected: switched to OFFLINE in {switch_latency:.2f}s (< 10s)")
        self.assertEqual(syncer.current_mode, "offline", "Pipeline failed to switch to OFFLINE within 10s")
        
        # Collect offline
        time.sleep(3.0)
        
        offline_results = device_storage.get_results("switch_session")
        self.assertGreater(len(offline_results), 0, "Device should process windows offline during network outage")
        
        stats_offline = device_storage.get_stats("switch_session")
        self.assertGreater(stats_offline["backlog"], 500, "Backlog must accumulate while cloud is down")
        
        # Phase C: Restore Network!
        restore_time = time.time()
        httpd_restored = ThreadingHTTPServer(("127.0.0.1", self.cloud_port), CloudRequestHandler)
        t_cloud_restored = threading.Thread(target=httpd_restored.serve_forever, daemon=True)
        t_cloud_restored.start()
        time.sleep(0.2)
        
        while syncer.current_mode != "online" and (time.time() - restore_time) < 10.0:
            time.sleep(0.1)
            
        online_switch_latency = time.time() - restore_time
        print(f"[Test] Network restore detected: switched to ONLINE in {online_switch_latency:.2f}s (< 10s)")
        self.assertEqual(syncer.current_mode, "online", "Pipeline failed to switch to ONLINE within 10s")
        
        reader.close()
        
        # Phase D: Wait for Catch-Up
        catchup_start = time.time()
        while time.time() - catchup_start < 10.0:
            stats = device_storage.get_stats("switch_session")
            if stats["backlog"] == 0:
                break
            syncer.step_sync()
            time.sleep(0.05)
            
        catchup_duration = time.time() - catchup_start
        stats_final = device_storage.get_stats("switch_session")
        print(f"[Test] Catch-up complete in {catchup_duration:.2f}s! Backlog remaining: {stats_final['backlog']}")
        self.assertEqual(stats_final["backlog"], 0, "All backlogged samples must be caught up to cloud")
        
        syncer.running = False
        mock_server.stop()
        httpd_restored.shutdown()
        
        # Phase E: Validate Ground Truth
        device_export = list(device_storage.export_data_generator("switch_session"))
        cloud_export = list(cloud_storage.export_data_generator("switch_session"))
        
        self.assertEqual(len(device_export), len(cloud_export), "Cloud copy must have identical count as device")
        
        valid_dev, errors_dev = verify_export(device_export, mode="ramp", seed=42, duration=60, expected_session="switch_session")
        self.assertTrue(valid_dev, f"Device export errors: {errors_dev[:3]}")
        
        valid_cloud, errors_cloud = verify_export(cloud_export, mode="ramp", seed=42, duration=60, expected_session="switch_session")
        self.assertTrue(valid_cloud, f"Cloud export errors: {errors_cloud[:3]}")
        print("[Test] SUCCESS: Zero loss, zero duplicates across network cut and catchup!")

    def test_mock_crc32_corruption_fault(self):
        """Test Section 6.3: mock sends corrupt reply (wrong crc32) -> must reject and re-read."""
        mock_server = MockServer(
            mode="ramp",
            seed=10,
            duration=10,
            port=self.mock_port,
            session_id="crc_fault_sess",
            fault_flags={"corrupt_crc": True}
        )
        mock_server.start(non_blocking=True)
        
        storage = DeviceStorage(self.device_db)
        reader = DeviceReader(storage=storage, mock_port=self.mock_port)
        
        res1 = reader.step_read(1)
        self.assertFalse(res1, "Reader must reject corrupt CRC reply")
        
        res2 = reader.step_read(1)
        self.assertTrue(res2, "Reader should successfully re-read with correct CRC")
        
        reader.close()
        mock_server.stop()

    def test_mock_duplicate_reply_fault(self):
        """Test Section 6.3: mock sends duplicate reply (same reply sent twice)."""
        mock_server = MockServer(
            mode="ramp",
            seed=11,
            duration=10,
            port=self.mock_port,
            session_id="dup_sess",
            fault_flags={"duplicate_reply": True}
        )
        mock_server.start(non_blocking=True)
        
        storage = DeviceStorage(self.device_db)
        reader = DeviceReader(storage=storage, mock_port=self.mock_port)
        
        # Read sensor 1: mock will send sensor 1 reply twice
        r1 = reader.step_read(1)
        self.assertTrue(r1)
        
        # Next read sensor 2: reader handles the duplicate reply cleanly
        # and proceeds to fetch sensor 2 without misassigning sensor 1 data into sensor 2!
        r2 = reader.step_read(2)
        self.assertTrue(r2)
        
        # Verify samples on disk
        dev_export = list(storage.export_data_generator("dup_sess"))
        valid, errors = verify_export(dev_export, mode="ramp", seed=11, duration=10, expected_session="dup_sess")
        self.assertTrue(valid, f"Duplicate handling resulted in invalid export: {errors[:3]}")
        
        reader.close()
        mock_server.stop()

    def test_process_crash_and_cursor_persistence(self):
        """Test Section 6.3: process kill / restart -> reader resumes from persisted disk cursor."""
        mock_server = MockServer(
            mode="ramp",
            seed=12,
            duration=15,
            port=self.mock_port,
            session_id="crash_sess"
        )
        mock_server.start(non_blocking=True)
        
        storage = DeviceStorage(self.device_db)
        reader1 = DeviceReader(storage=storage, mock_port=self.mock_port)
        
        # Read some data
        reader1.step_read(1)
        reader1.step_read(2)
        c1 = reader1.last_received_idx[1]
        c2 = reader1.last_received_idx[2]
        self.assertGreater(c1, 0)
        self.assertGreater(c2, 0)
        
        # Simulate process crash: close socket abruptly without shutdown
        reader1.close_socket()
        del reader1
        
        # Restart process: new Reader instance reading from same SQLite DB
        reader2 = DeviceReader(storage=storage, mock_port=self.mock_port)
        self.assertEqual(reader2.current_session_id, "crash_sess")
        self.assertEqual(reader2.last_received_idx[1], c1, "Cursor for sensor 1 should be resumed from disk")
        self.assertEqual(reader2.last_received_idx[2], c2, "Cursor for sensor 2 should be resumed from disk")
        
        # Continue reading
        reader2.step_read(1)
        reader2.step_read(2)
        
        # Verify export passes ground truth with zero duplicate or lost samples
        dev_export = list(storage.export_data_generator("crash_sess"))
        valid, errors = verify_export(dev_export, mode="ramp", seed=12, duration=15, expected_session="crash_sess")
        self.assertTrue(valid, f"Resume from disk produced invalid export: {errors[:3]}")
        
        reader2.close()
        mock_server.stop()

    def test_mock_restart_session_transition(self):
        """Test Section 6.3: mock restarts with new session -> unread data marked lost with to_index=null."""
        mock_server = MockServer(
            mode="ramp",
            seed=14,
            duration=15,
            port=self.mock_port,
            session_id="session_A"
        )
        mock_server.start(non_blocking=True)
        
        storage = DeviceStorage(self.device_db)
        reader = DeviceReader(storage=storage, mock_port=self.mock_port)
        
        # Collect for session A
        reader.step_read(1)
        reader.step_read(2)
        self.assertEqual(reader.current_session_id, "session_A")
        
        # Mock restarts with session B!
        mock_server.restart("session_B")
        
        # Read next: should detect session B and record unread data of session A as lost with to_index=null
        reader.step_read(1)
        self.assertEqual(reader.current_session_id, "session_B")
        
        # Check lost ranges for session A
        lost_ranges = storage.get_unsynced_lost("session_A")
        self.assertGreater(len(lost_ranges), 0, "Unread samples of old session must be recorded as lost")
        for lr in lost_ranges:
            self.assertIsNone(lr["to_index"], "Unread data must have to_index: null")
            self.assertEqual(lr["reason"], "mock_restart")
            
        reader.close()
        mock_server.stop()

if __name__ == "__main__":
    unittest.main()
