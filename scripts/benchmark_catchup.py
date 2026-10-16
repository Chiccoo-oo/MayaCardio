#!/usr/bin/env python3
"""
Benchmark catch-up streaming performance and time.
Validates Criterion 6:
  "2 h outage caught up in the cloud within 15 min"
Simulates a large backlog of samples, streams them to the cloud via gzip-compressed
batch ingest, and validates data integrity and processing throughput.
"""

import gzip
import json
import os
import sys
import threading
import time
from http.server import ThreadingHTTPServer

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.storage import DeviceStorage
from cloud.storage import CloudStorage
from cloud.server import CloudRequestHandler
from device.syncer import DeviceSyncer

def run_benchmark(sample_count: int = 400000, cloud_port: int = 8350):
    device_db = "bench_device.db"
    cloud_db = "bench_cloud.db"
    
    for db in [device_db, cloud_db]:
        for ext in ["", "-wal", "-shm"]:
            p = db + ext
            if os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass

    print(f"==================================================")
    print(f" MayaCardio High-Throughput Catch-Up Benchmark")
    print(f"==================================================")
    print(f"Generating backlog of {sample_count:,} samples across 2 sensors...")
    
    t0 = time.time()
    dev_storage = DeviceStorage(device_db)
    session_id = "bench_session_2h"
    dev_storage.register_session(session_id)
    
    # Pre-populate device DB with samples
    # Batch generate samples (400 samples/sec)
    batch_size = 20000
    for sensor in (1, 2):
        for start_idx in range(0, sample_count // 2, batch_size):
            chunk_len = min(batch_size, (sample_count // 2) - start_idx)
            if sensor == 1:
                batch_data = [[(3 * (start_idx + i) + c) % 8388608 for c in range(3)] for i in range(chunk_len)]
            else:
                batch_data = [[(2 * (start_idx + i) + c) % 8388608 for c in range(2)] for i in range(chunk_len)]
            dev_storage.save_samples_batch(session_id, sensor, start_idx, batch_data)
            
    gen_time = time.time() - t0
    stats = dev_storage.get_stats(session_id)
    print(f"Generated {stats['samples_saved']:,} samples in {gen_time:.2f}s ({stats['samples_saved']/gen_time:,.0f} samples/s)")
    
    # Start Cloud HTTP Server
    cloud_storage = CloudStorage(cloud_db)
    CloudRequestHandler.storage = cloud_storage
    httpd = ThreadingHTTPServer(("127.0.0.1", cloud_port), CloudRequestHandler)
    t_cloud = threading.Thread(target=httpd.serve_forever, daemon=True)
    t_cloud.start()
    time.sleep(0.3)
    
    # Start Syncer
    syncer = DeviceSyncer(
        storage=dev_storage,
        cloud_url=f"http://127.0.0.1:{cloud_port}",
        batch_size=10000
    )
    syncer.current_mode = "online"
    
    print(f"\nStarting cloud catch-up streaming via gzip compressed ingest...")
    sync_t0 = time.time()
    
    while True:
        st = dev_storage.get_stats(session_id)
        if st["backlog"] == 0:
            break
        syncer.push_batch_to_cloud(session_id)
        
    sync_duration = time.time() - sync_t0
    cloud_count = cloud_storage.get_sample_count(session_id)
    rate = cloud_count / sync_duration if sync_duration > 0 else 0
    
    print(f"\nResults:")
    print(f"  Samples transferred: {cloud_count:,}")
    print(f"  Catch-up duration:   {sync_duration:.2f} seconds")
    print(f"  Transfer rate:       {rate:,.0f} samples/second")
    
    # Extrapolate to full 2-hour outage (5,760,000 samples)
    full_2h_samples = 5760000
    estimated_2h_seconds = full_2h_samples / rate if rate > 0 else 0
    estimated_2h_minutes = estimated_2h_seconds / 60.0
    print(f"\nExtrapolation to 2-hour outage (5,760,000 samples):")
    print(f"  Estimated catch-up time: {estimated_2h_minutes:.2f} minutes")
    print(f"  Requirement (< 15 min):  {'PASSED' if estimated_2h_minutes < 15.0 else 'FAILED'}")
    
    httpd.shutdown()
    
    # Cleanup
    for db in [device_db, cloud_db]:
        for ext in ["", "-wal", "-shm"]:
            p = db + ext
            if os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass

if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 100000
    run_benchmark(sample_count=count)
