"""Sanity test for LeapClient with aurora-viewer.exe."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from leap_client import LeapClient


def main():
    print("Launching client...")
    client = LeapClient()
    try:
        print(f"Handshake complete! cmd_uuid={client.cmd_uuid}, reply_uuid={client.reply_uuid}")
        assert client.cmd_uuid is not None
        assert client.reply_uuid is not None
        assert len(client.cmd_uuid) == 36
        assert len(client.reply_uuid) == 36

        print("Testing ping on command pump...")
        resp = client.ping(reqid=4242)
        print("Ping response:", resp)
        assert resp.get("status") is True
        assert resp.get("reqid") == 4242

        print("Testing status on LLAppViewer...")
        status = client.request("LLAppViewer", op="status", reqid=123)
        print("LLAppViewer status response:", status)
        assert status.get("reqid") == 123
        assert "version" in status
        assert "uptime" in status

        print("Gracefully quitting...")
        exit_code = client.quit_gracefully(timeout=5.0)
        print(f"Viewer exited with code: {exit_code}")
        assert exit_code == 0
        print("SANITY TEST PASSED!")
    finally:
        if client.is_alive():
            client.terminate()


if __name__ == "__main__":
    main()
