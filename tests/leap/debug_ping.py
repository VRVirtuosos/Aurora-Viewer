"""Debug LEAP ping and stderr."""

import sys
import threading
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from leap_client import LeapClient


def main():
    c = LeapClient()
    stderr_lines = []

    def stderr_reader():
        while c.proc and c.proc.stderr:
            line = c.proc.stderr.readline()
            if not line:
                break
            stderr_lines.append(line.decode("utf-8", errors="replace"))

    t = threading.Thread(target=stderr_reader, daemon=True)
    t.start()

    print("Intro packet:", c.intro_packet)
    print("Sending ping...")
    c.send_message(c.cmd_uuid, {"op": "ping", "reply": c.reply_uuid, "reqid": 42})

    time.sleep(3.0)
    print("Total events received:", len(c._all_events))
    print("All events:", c._all_events)
    print("Stderr lines captured (first 20):")
    for l in stderr_lines[:20]:
        print("  ", l.strip())

    c.terminate()


if __name__ == "__main__":
    main()
