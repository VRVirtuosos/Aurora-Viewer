"""Test with sufficient warmup to allow Vulkan/OBS hook initialization."""

import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from leap_client import LeapClient


def main():
    print("Launching LeapClient...")
    t0 = time.time()
    c = LeapClient(startup_timeout=20.0)
    print(f"[{time.time()-t0:.2f}s] Handshake complete: cmd={c.cmd_uuid}, reply={c.reply_uuid}")

    # Wait 18 seconds to ensure Vulkan renderer has initialized past OBS hook
    print("Waiting 18 seconds for Vulkan renderer & first frame...")
    time.sleep(18.0)
    print(f"[{time.time()-t0:.2f}s] Warmup complete. Sending ping...")

    resp = c.ping(reqid=777, timeout=10.0)
    print(f"[{time.time()-t0:.2f}s] Ping reply received:", resp)

    print("Querying LLAppViewer status...")
    status = c.request("LLAppViewer", op="status", reqid=888, timeout=10.0)
    print(f"[{time.time()-t0:.2f}s] LLAppViewer status reply:", status)

    print("Gracefully quitting...")
    exit_code = c.quit_gracefully(timeout=5.0)
    print(f"[{time.time()-t0:.2f}s] Viewer exited with code: {exit_code}")


if __name__ == "__main__":
    main()
