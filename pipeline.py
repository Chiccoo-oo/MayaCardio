#!/usr/bin/env python3
"""
MayaCardio 'pipeline' single device CLI and daemon.
Implements all required commands:
  - pipeline status
  - pipeline export --session ID --from device|cloud
  - pipeline results --session ID
  - pipeline run (starts collector, syncer, and processor daemon)
"""

import argparse
import json
import os
import signal
import sys
import threading
import time
import urllib.request
import urllib.parse
from typing import Dict, Any, Optional, List

from core.storage import DeviceStorage
from device.reader import DeviceReader
from device.syncer import DeviceSyncer

DEFAULT_DB = os.environ.get("MAYA_DB", "maya_device.db")
DEFAULT_CLOUD_URL = os.environ.get("MAYA_CLOUD_URL", "http://127.0.0.1:8000")
DEFAULT_MOCK_HOST = os.environ.get("MAYA_MOCK_HOST", "127.0.0.1")
DEFAULT_MOCK_PORT = int(os.environ.get("MAYA_MOCK_PORT", "7100"))

def cmd_status(args):
    storage = DeviceStorage(db_path=args.db)
    active_session = storage.get_active_session()
    stats = storage.get_stats(active_session)
    
    # Check daemon reported mode in KV store first
    daemon_mode = storage.get_kv("mode")
    if daemon_mode in ("online", "offline"):
        mode = daemon_mode
    else:
        # Fall back to live probe
        cloud_url = getattr(args, "cloud_url", DEFAULT_CLOUD_URL).rstrip("/")
        is_online = False
        try:
            req = urllib.request.Request(f"{cloud_url}/health")
            with urllib.request.urlopen(req, timeout=1.5) as resp:
                if resp.status == 200:
                    is_online = True
        except Exception:
            is_online = False
        mode = "online" if is_online else "offline"
        
    output = {
        "mode": mode,
        "samples_saved": stats["samples_saved"],
        "samples_cloud": stats["samples_cloud"],
        "backlog": stats["backlog"]
    }
    print(json.dumps(output))

def cmd_export(args):
    session_id = args.session
    source = args.from_source
    
    if source == "device":
        storage = DeviceStorage(db_path=args.db)
        for line in storage.export_data_generator(session_id):
            print(line)
    elif source == "cloud":
        cloud_url = getattr(args, "cloud_url", DEFAULT_CLOUD_URL).rstrip("/")
        url = f"{cloud_url}/api/export?session={urllib.parse.quote(session_id)}"
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=30.0) as resp:
                for line in resp:
                    line_str = line.decode("utf-8").strip()
                    if line_str:
                        print(line_str)
        except Exception as e:
            sys.stderr.write(f"Error fetching export from cloud: {e}\n")
            sys.exit(1)
    else:
        sys.stderr.write(f"Unknown source: {source}. Must be 'device' or 'cloud'.\n")
        sys.exit(1)

def cmd_results(args):
    session_id = args.session
    storage = DeviceStorage(db_path=args.db)
    cloud_url = getattr(args, "cloud_url", DEFAULT_CLOUD_URL).rstrip("/")
    
    # Section 3 & 7 requirement: Print one line per window AND mode!
    # Merge cloud-processed results and device-processed offline results.
    merged: Dict[Tuple[int, str], Dict[str, Any]] = {}

    # 1. Device results
    for r in storage.get_results(session_id):
        key = (r["window"], r.get("mode", "offline"))
        merged[key] = r

    # 2. Cloud results (if cloud reachable)
    try:
        url = f"{cloud_url}/api/results?session={urllib.parse.quote(session_id)}"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            for line in resp:
                line_str = line.decode("utf-8").strip()
                if line_str:
                    item = json.loads(line_str)
                    key = (item["window"], item.get("mode", "online"))
                    merged[key] = item
    except Exception:
        pass

    # Sort results by window ASC, then mode ('offline' before 'online')
    sorted_keys = sorted(merged.keys(), key=lambda k: (k[0], 0 if k[1] == "offline" else 1))
    for k in sorted_keys:
        print(json.dumps(merged[k], separators=(',', ':')))

def cmd_run(args):
    storage = DeviceStorage(db_path=args.db)
    reader = DeviceReader(
        storage=storage,
        mock_host=args.mock_host,
        mock_port=args.mock_port
    )
    syncer = DeviceSyncer(
        storage=storage,
        cloud_url=args.cloud_url
    )
    
    t_reader = threading.Thread(target=reader.run_loop, daemon=True, name="Collector")
    t_syncer = threading.Thread(target=syncer.run_loop, daemon=True, name="Syncer")
    
    t_reader.start()
    t_syncer.start()
    
    print(f"[pipeline] Started collection and syncer daemon (PID {os.getpid()})")
    
    def shutdown(signum, frame):
        print("\n[pipeline] Shutting down gracefully...")
        reader.close()
        syncer.running = False
        sys.exit(0)
        
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    
    while True:
        time.sleep(1)

def build_parser():
    parser = argparse.ArgumentParser(prog="pipeline", description="MayaCardio bedside data pipeline")
    parser.add_argument("--db", type=str, default=DEFAULT_DB, help="Device database file")
    parser.add_argument("--cloud-url", type=str, default=DEFAULT_CLOUD_URL, help="Cloud service URL")
    parser.add_argument("--mock-host", type=str, default=DEFAULT_MOCK_HOST, help="biosignal-mock host")
    parser.add_argument("--mock-port", type=int, default=DEFAULT_MOCK_PORT, help="biosignal-mock port")
    
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # pipeline status
    p_status = subparsers.add_parser("status", help="Print pipeline status JSON")
    p_status.set_defaults(func=cmd_status)
    
    # pipeline export --session ID --from device|cloud
    p_export = subparsers.add_parser("export", help="Export session data")
    p_export.add_argument("--session", required=True, type=str, help="Session ID")
    p_export.add_argument("--from", dest="from_source", required=True, choices=["device", "cloud"], help="Source (device or cloud)")
    p_export.set_defaults(func=cmd_export)
    
    # pipeline results --session ID
    p_results = subparsers.add_parser("results", help="Print session results JSON")
    p_results.add_argument("--session", required=True, type=str, help="Session ID")
    p_results.set_defaults(func=cmd_results)
    
    # pipeline run
    p_run = subparsers.add_parser("run", help="Start device pipeline daemon")
    p_run.set_defaults(func=cmd_run)
    
    return parser

def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
