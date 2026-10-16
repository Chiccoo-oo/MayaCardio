"""
Unit tests validating that incremental O(1) processing in core/processing.py
produces identical results to the reference mathematical implementation.
"""

import unittest
from typing import Dict, List
from core.processing import (
    scan_peaks,
    compute_window,
    detect_peaks,
    compute_heart_rate,
    window_stats,
    round_val,
    SAMPLE_RATE,
    WINDOW_SIZE
)
from mock.biosignal_mock import BiosignalGenerator

class TestIncrementalProcessing(unittest.TestCase):
    def test_window_stats(self):
        s1 = {i: [100, 200, 300] for i in range(400)}
        s2 = {i: [30, 40] for i in range(400)}
        stats = window_stats(0, s1, s2)
        self.assertEqual(stats["missing_1"], 0)
        self.assertEqual(stats["missing_2"], 0)
        self.assertTrue(stats["complete"])
        self.assertEqual(stats["mean"]["sensor1"], [100.0, 200.0, 300.0])
        self.assertEqual(stats["mean"]["sensor2"], [30.0, 40.0])
        self.assertEqual(stats["magnitude"], 50.0)

    def test_incremental_matches_reference_on_signal(self):
        gen = BiosignalGenerator(mode="signal", seed=7, duration_sec=12)
        
        # Build samples dict
        s1: Dict[int, List[int]] = {}
        s2: Dict[int, List[int]] = {}
        for i in range(12 * SAMPLE_RATE):
            s1[i] = gen.get_sample(1, i)
            s2[i] = gen.get_sample(2, i)

        ch2 = {i: s1[i][1] for i in range(len(s1))}
        
        # Reference peaks over entire buffer
        ref_peaks = detect_peaks(ch2, 800, len(s1) - 2)
        
        # Incremental scan_peaks per window
        inc_peaks: List[int] = []
        last_peak = None
        for w in range(2, 11):
            start = 400 * w
            end = 400 * w + 399
            w_peaks = scan_peaks(ch2, start, end, last_peak)
            inc_peaks.extend(w_peaks)
            if w_peaks:
                last_peak = w_peaks[-1]
                
        # Compare peaks detected in the tested window range
        expected_in_range = [p for p in ref_peaks if 400 * 2 <= p <= 400 * 10 + 399]
        self.assertEqual(inc_peaks, expected_in_range, "Incremental scan_peaks must match reference detect_peaks")

    def test_compute_window_sequence(self):
        gen = BiosignalGenerator(mode="signal", seed=42, duration_sec=8)
        s1 = {i: gen.get_sample(1, i) for i in range(8 * SAMPLE_RATE)}
        s2 = {i: gen.get_sample(2, i) for i in range(8 * SAMPLE_RATE)}
        
        last_peak = None
        recent_peaks = []
        recent_missing = []
        
        for w in range(7):
            res, last_peak, recent_peaks, recent_missing = compute_window(
                window=w,
                session_id="test_sess",
                s1=s1,
                s2=s2,
                last_peak=last_peak,
                recent_peaks=recent_peaks,
                recent_missing=recent_missing,
                mode="online"
            )
            self.assertEqual(res["window"], w)
            self.assertEqual(res["mode"], "online")
            self.assertEqual(res["missing_1"], 0)
            self.assertEqual(res["missing_2"], 0)
            if w >= 4:
                # Windows 4+ have 5 full seconds of history -> valid heart rate
                self.assertIsNotNone(res["heart_rate"])
                self.assertGreater(res["heart_rate"], 40.0)
                self.assertLess(res["heart_rate"], 150.0)
            else:
                self.assertIsNone(res["heart_rate"])

if __name__ == "__main__":
    unittest.main()
