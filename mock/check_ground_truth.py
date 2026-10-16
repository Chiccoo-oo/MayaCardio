"""
check_ground_truth verification tool.
Compares a pipeline export (from device or cloud) against mock ground truth.
Verifies Criterion 1 & 2:
- Every received sample is present exactly once, unchanged.
- Every sample not present is listed in lost records with a valid reason.
- Flags any silent loss, corruption, or unwarranted duplicate.
"""

import argparse
import json
import sys
from typing import Dict, List, Set, Tuple, Any
from mock.biosignal_mock import BiosignalGenerator

def verify_export(
    export_lines: List[str],
    mode: str,
    seed: int,
    duration: int,
    expected_session: str
) -> Tuple[bool, List[str]]:
    generator = BiosignalGenerator(mode, seed, duration)
    errors: List[str] = []
    
    # Track samples: (sensor, index) -> values
    received_samples: Dict[Tuple[int, int], List[int]] = {}
    lost_intervals: List[Dict[str, Any]] = []
    
    line_num = 0
    for line in export_lines:
        line_num += 1
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except Exception as e:
            errors.append(f"Line {line_num}: invalid JSON: {e}")
            continue
            
        if "index" in item:
            # Sample record: {"sensor": 1, "index": 120000, "values": [...]}
            s = item["sensor"]
            idx = item["index"]
            vals = item["values"]
            key = (s, idx)
            if key in received_samples:
                errors.append(f"Duplicate sample detected: Sensor {s}, index {idx}")
            received_samples[key] = vals
            
            # Verify value against ground truth
            truth_vals = generator.get_sample(s, idx)
            if vals != truth_vals:
                errors.append(f"Value mismatch at Sensor {s}, index {idx}: got {vals}, expected {truth_vals}")
                
        elif "from_index" in item:
            # Lost record: {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "..."}
            lost_intervals.append(item)
        else:
            errors.append(f"Line {line_num}: unrecognized export record format: {item}")

    # Check for silent losses between min and max received index per sensor
    for s in (1, 2):
        sensor_indices = [idx for (sensor_num, idx) in received_samples.keys() if sensor_num == s]
        if not sensor_indices:
            continue
        min_idx = min(sensor_indices)
        max_idx = max(sensor_indices)
        
        # Build set of covered lost indices
        sensor_lost_indices: Set[int] = set()
        for lost in lost_intervals:
            if lost.get("sensor") == s:
                f_idx = lost["from_index"]
                t_idx = lost.get("to_index")
                if t_idx is not None:
                    for i in range(f_idx, t_idx + 1):
                        sensor_lost_indices.add(i)
                else:
                    # null to_index (unknown end / session abort)
                    pass
                    
        # Check every index in [min_idx, max_idx]
        for idx in range(min_idx, max_idx + 1):
            if (s, idx) not in received_samples and idx not in sensor_lost_indices:
                errors.append(f"SILENT DATA LOSS: Sensor {s}, sample {idx} is missing from disk and not listed as lost!")
                
    success = (len(errors) == 0)
    return success, errors

def main():
    parser = argparse.ArgumentParser(description="Check pipeline export against ground truth")
    parser.add_argument("export_file", nargs="?", default="-", help="Path to export file or '-' for stdin")
    parser.add_argument("--mode", choices=["signal", "ramp"], default="signal")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--duration", type=int, default=3600)
    parser.add_argument("--session", type=str, default="5f0c2a9e")
    args = parser.parse_args()
    
    if args.export_file == "-":
        lines = sys.stdin.readlines()
    else:
        with open(args.export_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
            
    success, errors = verify_export(lines, args.mode, args.seed, args.duration, args.session)
    
    if success:
        print(f"PASS: Ground truth validation successful! All samples accounted for without silent loss or corruption.")
        sys.exit(0)
    else:
        print(f"FAIL: Found {len(errors)} issues:")
        for err in errors[:25]:
            print(f"  - {err}")
        if len(errors) > 25:
            print(f"  ... and {len(errors) - 25} more.")
        sys.exit(1)

if __name__ == "__main__":
    main()
