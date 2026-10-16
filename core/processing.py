"""
Section 3 processing logic, shared by the device (offline) and the cloud (online).

Window w holds samples 400w .. 400w+399 of each sensor.

Two implementations of the heart-rate rule live here:
  * detect_peaks / compute_heart_rate: a direct, slow transcription of the rule.
    Used only as a reference in tests.
  * scan_peaks / compute_window: the incremental version used in production.
    It walks windows in order, keeps a tiny state (last peak, recent peaks,
    recent missing counts) and uses a monotonic deque for the 800-sample max,
    so each window costs O(1200) no matter how long the session is.
"""

import math
from collections import deque
from typing import Any, Dict, List, Optional, Sequence, Tuple

SAMPLE_RATE = 400
WINDOW_SIZE = 400
PEAK_LOOKBACK = 800
PEAK_MIN_GAP = 100
HR_WINDOWS = 5  # the last 5 seconds: windows w-4 .. w


def round_val(val: Optional[float]) -> Optional[float]:
    """Round decimal results to 1 decimal place (Section 3)."""
    if val is None:
        return None
    return round(float(val), 1)


# --------------------------------------------------------------------------
# Reference implementation (slow, literal transcription of the rule)
# --------------------------------------------------------------------------
def detect_peaks(samples_s1_c2: Dict[int, int], start_idx: int, end_idx: int) -> List[int]:
    peaks: List[int] = []
    for i in range(start_idx, end_idx + 1):
        if i not in samples_s1_c2 or (i + 1) not in samples_s1_c2:
            continue
        if any(k not in samples_s1_c2 for k in range(i - PEAK_LOOKBACK, i)):
            continue
        v = samples_s1_c2[i]
        if not (v > samples_s1_c2[i - 1] and v > samples_s1_c2[i + 1]):
            continue
        if not (v > 0.5 * max(samples_s1_c2[k] for k in range(i - PEAK_LOOKBACK, i))):
            continue
        if peaks and i - peaks[-1] < PEAK_MIN_GAP:
            continue
        peaks.append(i)
    return peaks


def compute_heart_rate(samples_s1_c2: Dict[int, int], window: int) -> Optional[float]:
    if window < 4:
        return None
    start_5s = WINDOW_SIZE * (window - 4)
    end_5s = WINDOW_SIZE * (window + 1) - 1
    if any(idx not in samples_s1_c2 for idx in range(start_5s, end_5s + 1)):
        return None
    peaks = [p for p in detect_peaks(samples_s1_c2, PEAK_LOOKBACK, end_5s) if p >= start_5s]
    if len(peaks) < 2:
        return None
    mean_gap = (peaks[-1] - peaks[0]) / (len(peaks) - 1) / SAMPLE_RATE
    return round_val(60.0 / mean_gap)


# --------------------------------------------------------------------------
# Production implementation (incremental)
# --------------------------------------------------------------------------
def scan_peaks(ch2: Dict[int, int], start: int, end: int, last_peak: Optional[int]) -> List[int]:
    """
    Peaks with index in [start, end]. `ch2` must hold every available sensor-1
    channel-2 value in [start-800, end+1]; absent keys mean "not available".
    `last_peak` is the previous peak found before `start` (or None).
    """
    peaks: List[int] = []
    dq: deque = deque()  # indices with strictly decreasing values over [i-800, i-1]
    missing = 0          # number of unavailable samples in [i-800, i-1]

    for k in range(start - PEAK_LOOKBACK, start):
        v = ch2.get(k) if k >= 0 else None
        if v is None:
            missing += 1
        else:
            while dq and ch2[dq[-1]] <= v:
                dq.pop()
            dq.append(k)

    for i in range(start, end + 1):
        v = ch2.get(i)
        if v is not None and missing == 0:
            nxt = ch2.get(i + 1)
            prv = ch2.get(i - 1)
            if (nxt is not None and prv is not None and v > prv and v > nxt
                    and v > 0.5 * ch2[dq[0]]
                    and (last_peak is None or i - last_peak >= PEAK_MIN_GAP)):
                peaks.append(i)
                last_peak = i
        # slide the look-back window forward by one: drop i-800, add i
        out = i - PEAK_LOOKBACK
        if out < 0 or ch2.get(out) is None:
            missing -= 1
        elif dq and dq[0] == out:
            dq.popleft()
        if v is None:
            missing += 1
        else:
            while dq and ch2[dq[-1]] <= v:
                dq.pop()
            dq.append(i)
    return peaks


def window_stats(window: int, s1: Dict[int, Sequence[int]], s2: Dict[int, Sequence[int]]) -> Dict[str, Any]:
    start = WINDOW_SIZE * window
    end = start + WINDOW_SIZE - 1
    avail_1 = [s1[i] for i in range(start, end + 1) if i in s1]
    avail_2 = [s2[i] for i in range(start, end + 1) if i in s2]
    missing_1 = WINDOW_SIZE - len(avail_1)
    missing_2 = WINDOW_SIZE - len(avail_2)

    def per_channel(avail: List[Sequence[int]], channels: int) -> Tuple[list, list, list]:
        if not avail:
            return [None] * channels, [None] * channels, [None] * channels
        means, mins, maxs = [], [], []
        for c in range(channels):
            vals = [s[c] for s in avail]
            means.append(round_val(sum(vals) / len(vals)))
            mins.append(round_val(min(vals)))
            maxs.append(round_val(max(vals)))
        return means, mins, maxs

    m1, n1, x1 = per_channel(avail_1, 3)
    m2, n2, x2 = per_channel(avail_2, 2)
    magnitude = None
    if avail_2:
        magnitude = round_val(sum(math.sqrt(s[0] ** 2 + s[1] ** 2) for s in avail_2) / len(avail_2))
    return {
        "missing_1": missing_1,
        "missing_2": missing_2,
        "complete": missing_1 == 0 and missing_2 == 0,
        "mean": {"sensor1": m1, "sensor2": m2},
        "min": {"sensor1": n1, "sensor2": n2},
        "max": {"sensor1": x1, "sensor2": x2},
        "magnitude": magnitude,
    }


def compute_window(
    window: int,
    session_id: str,
    s1: Dict[int, Sequence[int]],
    s2: Dict[int, Sequence[int]],
    last_peak: Optional[int],
    recent_peaks: List[int],
    recent_missing: List[int],
    mode: str,
) -> Tuple[Dict[str, Any], Optional[int], List[int], List[int]]:
    """
    Process one window. Windows MUST be fed in order 0, 1, 2, ...
    s1 must contain sensor-1 samples in [400w-800, 400w+400]; s2 sensor-2 samples of the window.
    Returns (result, last_peak, recent_peaks, recent_missing) - the latter three are the state
    to pass to the next window.
    """
    start = WINDOW_SIZE * window
    end = start + WINDOW_SIZE - 1
    stats = window_stats(window, s1, s2)

    ch2 = {i: v[1] for i, v in s1.items()}
    new_peaks = scan_peaks(ch2, start, end, last_peak)
    if new_peaks:
        last_peak = new_peaks[-1]

    hr_start = WINDOW_SIZE * (window - 4)
    peaks = [p for p in recent_peaks if p >= hr_start] + new_peaks
    missing_hist = (list(recent_missing) + [stats["missing_1"]])[-HR_WINDOWS:]

    heart_rate = None
    if window >= 4 and len(missing_hist) == HR_WINDOWS and not any(missing_hist) and len(peaks) >= 2:
        mean_gap_s = (peaks[-1] - peaks[0]) / (len(peaks) - 1) / SAMPLE_RATE
        heart_rate = round_val(60.0 / mean_gap_s)

    result = {"window": window, "session_id": session_id}
    result.update(stats)
    result["heart_rate"] = heart_rate
    result["mode"] = mode

    keep_from = WINDOW_SIZE * (window - 3)  # what window w+1 will need
    return result, last_peak, [p for p in peaks if p >= keep_from], missing_hist[-(HR_WINDOWS - 1):]


def process_window(window, session_id, s1_samples, s2_samples, all_s1_c2_history, mode="online"):
    """Stateless convenience wrapper (uses the reference heart-rate rule). Kept for tests."""
    stats = window_stats(window, s1_samples, s2_samples)
    result = {"window": window, "session_id": session_id}
    result.update(stats)
    result["heart_rate"] = compute_heart_rate(all_s1_c2_history, window)
    result["mode"] = mode
    return result
