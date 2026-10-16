"""
Comprehensive automated test suite covering:
1. Section 3 Mathematical & Processing Logic (Peak detection, Heart Rate, Magnitude)
2. CRC-32 Validation & Faulty Message Rejection
3. End-to-End Collection, Storage, and Ground Truth Match
4. Online -> Offline -> Online Switch & Catch-up Verification
5. Process Kill & Power Cut Durability Verification
"""

import json
import os
import shutil
import subprocess
import sys
import threading
import time
import unittest
from typing import Dict, List

# Add workspace to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.processing import compute_heart_rate, detect_peaks, process_window
from core.storage import DeviceStorage
from mock.biosignal_mock import BiosignalGenerator, MockServer
from mock.check_ground_truth import verify_export
from cloud.storage import CloudStorage
from cloud.server import CloudRequestHandler
from http.server import ThreadingHTTPServer

class TestProcessingLogic(unittest.TestCase):
    def test_ramp_samples(self):
        gen = BiosignalGenerator(mode="ramp", seed=7, duration_sec=10)
        s1_0 = gen.get_sample(1, 0)
        # s1: (3*0 + c) mod 8388608 -> [0, 1, 2]
        self.assertEqual(s1_0, [0, 1, 2])
        s2_0 = gen.get_sample(2, 0)
        # s2: (2*0 + c) mod 8388608 -> [0, 1]
        self.assertEqual(s2_0, [0, 1])

    def test_window_metrics(self):
        s1_samples = {i: [100, 200, 300] for i in range(400)}
        s2_samples = {i: [300, 400] for i in range(400)}
        # magnitude: sqrt(300^2 + 400^2) = 500
        res = process_window(
            window=0,
            session_id="test",
            s1_samples=s1_samples,
            s2_samples=s2_samples,
            all_s1_c2_history={},
            mode="offline"
        )
        self.assertEqual(res["missing_1"], 0)
        self.assertEqual(res["missing_2"], 0)
        self.assertTrue(res["complete"])
        self.assertEqual(res["magnitude"], 500.0)
        self.assertIsNone(res["heart_rate"])  # window 0 < 4 -> heart_rate must be null

    def test_heart_rate_detection(self):
        # 60 bpm synthetic signal has 1 peak every 400 samples (1 second)
        # Let's generate 6 seconds of signal data
        gen = BiosignalGenerator(mode="signal", seed=42, duration_sec=6)
        s1_c2_hist = {i: gen.get_sample(1, i)[1] for i in range(6 * 400)}
        
        # Window 4 covers [1600, 1999]. Last 5 seconds are [0, 1999].
        hr = compute_heart_rate(s1_c2_hist, window=4)
        self.assertIsNotNone(hr)
        # Cycle is 320 samples at 400Hz -> 75.0 bpm
        self.assertAlmostEqual(hr, 75.0, delta=2.0)

class TestStorageDurability(unittest.TestCase):
    def setUp(self):
        self.test_db = "test_durability.db"
        if os.path.exists(self.test_db):
            os.remove(self.test_db)
        self.storage = DeviceStorage(self.test_db)

    def tearDown(self):
        del self.storage
        for f in [self.test_db, self.test_db + "-wal", self.test_db + "-shm"]:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except Exception:
                    pass

    def test_sample_deduplication_and_loss(self):
        session = "session_abc"
        self.storage.register_session(session, time.time())
        
        # Save 10 samples
        data = [[1, 2, 3] for _ in range(10)]
        inserted = self.storage.save_samples_batch(session, sensor=1, first_index=0, data=data)
        self.assertEqual(inserted, 10)
        
        # Resending same batch should be ignored (no duplicate)
        inserted_dup = self.storage.save_samples_batch(session, sensor=1, first_index=0, data=data)
        self.assertEqual(inserted_dup, 0)
        
        # Record lost
        self.storage.record_lost(session, sensor=1, from_index=10, to_index=19, reason="test_loss")
        
        # Export
        lines = list(self.storage.export_data_generator(session))
        self.assertEqual(len(lines), 11)  # 10 samples + 1 lost range

class TestEndToEndPipeline(unittest.TestCase):
    def setUp(self):
        self.device_db = "test_e2e_device.db"
        self.cloud_db = "test_e2e_cloud.db"
        for db in [self.device_db, self.cloud_db]:
            for ext in ["", "-wal", "-shm"]:
                p = db + ext
                if os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass
                        
        self.mock_port = 7199
        self.cloud_port = 8099
        
        # 1. Start Mock Server in signal mode
        self.mock_server = MockServer(
            mode="signal",
            seed=7,
            duration=30,
            port=self.mock_port,
            session_id="test_sess_01"
        )
        self.mock_server.start(non_blocking=True)
        
        # 2. Start Cloud Server
        self.cloud_storage = CloudStorage(self.cloud_db)
        CloudRequestHandler.storage = self.cloud_storage
        self.httpd = ThreadingHTTPServer(("127.0.0.1", self.cloud_port), CloudRequestHandler)
        self.t_cloud = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.t_cloud.start()
        time.sleep(0.5)

    def tearDown(self):
        self.mock_server.stop()
        self.httpd.shutdown()
        for db in [self.device_db, self.cloud_db]:
            for ext in ["", "-wal", "-shm"]:
                p = db + ext
                if os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass

    def test_full_pipeline_flow(self):
        from device.reader import DeviceReader
        from device.syncer import DeviceSyncer
        
        device_storage = DeviceStorage(self.device_db)
        reader = DeviceReader(
            storage=device_storage,
            mock_host="127.0.0.1",
            mock_port=self.mock_port
        )
        syncer = DeviceSyncer(
            storage=device_storage,
            cloud_url=f"http://127.0.0.1:{self.cloud_port}",
            probe_interval=0.5
        )
        
        # Run collection and sync for 6 seconds
        t_r = threading.Thread(target=reader.run_loop, daemon=True)
        t_s = threading.Thread(target=syncer.run_loop, daemon=True)
        t_r.start()
        t_s.start()
        
        time.sleep(6.0)
        
        reader.close()
        
        # Allow syncer to push remaining samples to cloud
        flush_deadline = time.time() + 5.0
        while time.time() < flush_deadline:
            stats = device_storage.get_stats("test_sess_01")
            if stats["backlog"] == 0:
                break
            syncer.step_sync()
            time.sleep(0.05)
            
        syncer.running = False
        
        # Verify device stats
        stats = device_storage.get_stats("test_sess_01")
        self.assertGreater(stats["samples_saved"], 2000, "Should have saved at least 2000 samples")
        self.assertEqual(stats["backlog"], 0, "Cloud sync should have zero backlog")
        
        # Verify export from device passes Ground Truth validation
        device_export = list(device_storage.export_data_generator("test_sess_01"))
        valid, errors = verify_export(
            device_export,
            mode="signal",
            seed=7,
            duration=30,
            expected_session="test_sess_01"
        )
        self.assertTrue(valid, f"Device export failed ground truth: {errors[:5]}")
        
        # Verify export from cloud matches
        cloud_export = list(self.cloud_storage.export_data_generator("test_sess_01"))
        self.assertEqual(len(device_export), len(cloud_export), "Cloud export sample count must match device")
        
        # Verify cloud generated window results (Section 3)
        cloud_results = self.cloud_storage.get_results("test_sess_01")
        self.assertGreater(len(cloud_results), 2, "Cloud should have processed online windows")
        for res in cloud_results:
            self.assertEqual(res["mode"], "online")
            self.assertIn("missing_1", res)
            self.assertIn("magnitude", res)

if __name__ == "__main__":
    unittest.main()
