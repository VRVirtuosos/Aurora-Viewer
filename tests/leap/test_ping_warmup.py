"""Test ping with proper warmup."""

import sys
import threading
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from leap_client import LeapClient


def main():
    print("Launching LeapClient...")
    c = LeapClient(startup_timeout=15.0)
    print(f"Handshake OK: cmd={c.cmd_uuid}, reply={c.reply_uuid}")

    # Wait for the first frame to render (give it 3 seconds)
    print("Waiting 4 seconds for window & first frame...")
    time.sleep(4.0)

    print("Sending ping...")
    resp = c.ping(reqid=777, timeout=10.0)
    print("Ping reply received:", resp)

    print("Querying LLAppViewer status...")
    status = c.request("LLAppViewer", op="status", reqid=888, timeout=10.0)
    print("LLAppViewer status reply:", status)

    print("Sending requestQuit...")
    exit_code = c.quit_gracefully(timeout=5.0)
    print(f"Clean exit with code: {exit_code}")


if __name__ == "__main__":
    main()
