"""Unit tests for the pure PLC + haptic-backoff logic (no network, no Redis)."""
import time
import importlib

cs = importlib.import_module("cue_service")


def test_offset_numeric_default():
    st = cs.ClientLatencyState()                       # no samples => default rtt
    out = cs.compute_client_offset(server_ts=1000.0, state=st, now=1000.0)   # network-only (elapsed 0)
    expected = cs.CFG.plc_safety_margin_ms - cs.CFG.plc_default_rtt_ms / 2.0   # 200 - 60 = 140
    assert abs(out["offset_ms"] - expected) < 0.1      # catches formula regression even though in-bounds
    assert out["client_latency_ms"] == cs.CFG.plc_default_rtt_ms
    assert out["target_ts"] == out["base_target_ts"]   # visual == haptic sync guarantee


def test_elapsed_is_compensated():
    st = cs.ClientLatencyState()                       # default one_way 60
    out = cs.compute_client_offset(server_ts=1000.0, state=st, now=1000.1)   # 100ms already elapsed
    assert abs(out["offset_ms"] - (200 - 100 - 60)) < 0.5     # ~40: queue/processing delay is compensated


def test_high_latency_clamps_to_floor():
    st = cs.ClientLatencyState()
    for _ in range(10):
        st.update_rtt(900.0)                           # one_way 450 > safety_margin 200
    out = cs.compute_client_offset(server_ts=0.0, state=st, now=0.0)
    assert out["offset_ms"] == cs.CFG.plc_min_offset_ms  # clamped (fire-on-receipt), never overshoot
    assert out["latency_bucket"] == ">300"


def test_ewma_convergence_from_different_start():
    st = cs.ClientLatencyState()
    st.update_rtt(50.0)                                # seeds avg=50
    st.update_rtt(150.0)                               # avg += 0.2*100 => 70
    assert st.last_rtt_ms == 150.0
    assert abs(st.rtt_avg_ms - 70.0) < 1e-6
    assert st.jitter_ms > 0                            # |delta| tracker engaged


def test_latency_bucket_boundaries():
    def bucket(r):
        s = cs.ClientLatencyState(); s.update_rtt(r); return s.latency_bucket
    assert bucket(49.9) == "0-50"
    assert bucket(50.0) == "50-150"                    # boundary is exclusive-low
    assert bucket(150.0) == "150-300"
    assert bucket(300.0) == ">300"


def test_haptic_strong_count_cap():
    conn = cs.ClientConn(ws=None, cid="t", ip="x")     # ws unused by haptic logic
    allowed = 0
    for _ in range(cs.CFG.haptic_strong_max + 3):
        if conn.haptic_allowed():
            conn.note_strong_haptic()
            allowed += 1
    assert allowed == cs.CFG.haptic_strong_max         # capped within the window


def test_haptic_window_expiry_frees_capacity():
    conn = cs.ClientConn(ws=None, cid="t", ip="x")
    old = time.time() - cs.CFG.haptic_strong_window_s - 1
    for _ in range(cs.CFG.haptic_strong_max):
        conn.strong_haptics.append(old)                # fill with EXPIRED strong haptics
    assert conn.haptic_allowed() is True               # evicted -> capacity freed (popleft branch)
