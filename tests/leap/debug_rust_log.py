"""Debug with RUST_LOG=leap=trace."""

import os
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

os.environ["RUST_LOG"] = "leap=trace,info"

from leap_client import LeapClient


def main():
    print("Launching with RUST_LOG=leap=trace,info...")
    c = LeapClient(startup_timeout=15.0)
    print(f"Handshake complete: cmd={c.cmd_uuid}, reply={c.reply_uuid}")

    print("Sending ping...")
    c.send_message(c.cmd_uuid, {"op": "ping", "reply": c.reply_uuid, "reqid": 1234})

    # Wait 5 seconds
    time.sleep(5.0)
    print("Closing client...")
    c.terminate()


if __name__ == "__main__":
    main()
