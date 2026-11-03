"""Verify ping and rapid subsequent requests once first frame renders."""

import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from leap_client import LeapClient


def main():
    print("Launching LeapClient...")
    t0 = time.time()
    c = LeapClient(startup_timeout=30.0)
    print(f"[{time.time()-t0:.2f}s] Handshake complete: cmd={c.cmd_uuid}, reply={c.reply_uuid}")

    print("Sending initial ping with 45s timeout to await first frame...")
    t_ping = time.time()
    resp = c.ping(reqid=1001, timeout=45.0)
    print(f"[{time.time()-t0:.2f}s] Initial ping returned in {time.time()-t_ping:.2f}s: {resp}")
    assert resp.get("status") is True
    assert resp.get("reqid") == 1001

    print("Subsequent commands should now respond instantaneously (< 0.1s):")

    # Command 2: LLAppViewer.status
    t_sub = time.time()
    status = c.request("LLAppViewer", op="status", reqid=1002, timeout=5.0)
    print(f"LLAppViewer.status in {time.time()-t_sub:.4f}s: {status}")
    assert status.get("reqid") == 1002
    assert "version" in status

    # Command 3: LLViewerControl.get DrawDistance
    t_sub = time.time()
    ctrl = c.request("LLViewerControl", op="get", data={"key": "DrawDistance"}, reqid=1003, timeout=5.0)
    print(f"LLViewerControl.get in {time.time()-t_sub:.4f}s: {ctrl}")
    assert ctrl.get("reqid") == 1003
    assert ctrl.get("key") == "DrawDistance"

    # Command 4: Ping 2
    t_sub = time.time()
    p2 = c.ping(reqid=1004, timeout=5.0)
    print(f"Ping 2 in {time.time()-t_sub:.4f}s: {p2}")
    assert p2.get("status") is True
    assert p2.get("reqid") == 1004

    # Graceful exit
    print("Requesting graceful quit...")
    t_quit = time.time()
    exit_code = c.quit_gracefully(timeout=5.0)
    print(f"[{time.time()-t0:.2f}s] Clean exit in {time.time()-t_quit:.2f}s with code: {exit_code}")
    assert exit_code == 0
    print("ALL TESTS IN FIRST_FRAME PASSED!")


if __name__ == "__main__":
    main()
