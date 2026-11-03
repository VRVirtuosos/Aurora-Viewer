"""
LEAP Automated Test Suite - Tier 1: Feature Coverage.

Covers:
  - Intro handshake reception and UUID validation.
  - Core ping / pong with verbatim reqid correlation.
  - Introspection enumeration (getAPIs) and API schema inspection (getAPI).
  - LLViewerControl configuration get, set, toggle, groups, vars.
  - Subsystem status queries (LLAppViewer, LLAgent, LLLogin, LLCamera, LLInventory).
"""

import sys
import time
from pathlib import Path
from typing import Any, Dict

sys.path.insert(0, str(Path(__file__).parent))
from leap_client import LeapClient


def run_tier1_tests(client: LeapClient) -> Dict[str, Any]:
    """Run all Tier 1 tests against an active LeapClient instance."""
    results = {}
    print("\n=== Running Tier 1: Feature Coverage ===")

    # 1.1 Intro Handshake Reception & UUID Parsing
    t0 = time.time()
    try:
        assert client.cmd_uuid is not None, "Missing command pump UUID"
        assert client.reply_uuid is not None, "Missing reply pump UUID"
        assert len(client.cmd_uuid) == 36, f"Invalid cmd UUID format: {client.cmd_uuid}"
        assert len(client.reply_uuid) == 36, f"Invalid reply UUID format: {client.reply_uuid}"
        assert isinstance(client.features, dict), "Features should be an LLSD Map"
        results["test_1_1_handshake"] = {
            "status": "PASS",
            "duration": time.time() - t0,
            "cmd_uuid": client.cmd_uuid,
            "reply_uuid": client.reply_uuid,
        }
        print("  [PASS] 1.1 Intro Handshake & UUID Parsing")
    except Exception as err:
        results["test_1_1_handshake"] = {"status": "FAIL", "error": str(err)}
        print(f"  [FAIL] 1.1 Intro Handshake: {err}")

    # 1.2 Core Ping / Pong with Verbatim reqid Correlation
    t0 = time.time()
    try:
        # Integer reqid
        p1 = client.ping(reqid=10101, timeout=5.0)
        assert p1.get("status") is True, f"Ping returned false: {p1}"
        assert p1.get("reqid") == 10101, f"reqid mismatch: expected 10101, got {p1.get('reqid')}"

        # String reqid
        p2 = client.ping(reqid="ping-str-correlation-999", timeout=5.0)
        assert p2.get("status") is True
        assert p2.get("reqid") == "ping-str-correlation-999"

        results["test_1_2_ping_reqid"] = {"status": "PASS", "duration": time.time() - t0}
        print("  [PASS] 1.2 Core Ping / Pong & Verbatim reqid Correlation")
    except Exception as err:
        results["test_1_2_ping_reqid"] = {"status": "FAIL", "error": str(err)}
        print(f"  [FAIL] 1.2 Core Ping: {err}")

    # 1.3 Introspection - getAPIs Enumeration
    t0 = time.time()
    try:
        apis_resp = client.command("getAPIs", reqid=102, timeout=5.0)
        assert apis_resp.get("reqid") == 102
        apis = apis_resp.get("apis", {})
        assert isinstance(apis, dict), f"apis field should be a map: {apis_resp}"

        expected_apis = [
            "LLAppViewer",
            "LLViewerControl",
            "LLChat",
            "LLAgent",
            "LLLogin",
            "LLCamera",
            "LLInventory",
        ]
        for api_name in expected_apis:
            assert api_name in apis, f"Expected API '{api_name}' missing from getAPIs: {list(apis.keys())}"
            assert "desc" in apis[api_name], f"API '{api_name}' missing description"

        results["test_1_3_get_apis"] = {
            "status": "PASS",
            "duration": time.time() - t0,
            "api_count": len(apis),
        }
        print(f"  [PASS] 1.3 Introspection getAPIs ({len(apis)} APIs discovered)")
    except Exception as err:
        results["test_1_3_get_apis"] = {"status": "FAIL", "error": str(err)}
        print(f"  [FAIL] 1.3 Introspection getAPIs: {err}")

    # 1.4 Introspection - getAPI Schema Inspection
    t0 = time.time()
    try:
        # LLViewerControl
        vc_meta = client.command("getAPI", {"api": "LLViewerControl"}, reqid=103, timeout=5.0)
        assert vc_meta.get("name") == "LLViewerControl"
        assert vc_meta.get("key") == "op"
        ops = vc_meta.get("ops", [])
        op_names = [op["name"] if isinstance(op, dict) else op for op in ops]
        for expected_op in ["get", "set", "toggle", "groups", "vars"]:
            assert expected_op in op_names, f"LLViewerControl missing op '{expected_op}'"

        # LLAppViewer
        app_meta = client.command("getAPI", {"api": "LLAppViewer"}, reqid=104, timeout=5.0)
        assert app_meta.get("name") == "LLAppViewer"
        assert app_meta.get("key") == "op"
        app_ops = [op["name"] if isinstance(op, dict) else op for op in app_meta.get("ops", [])]
        for expected_op in ["ping", "status", "requestQuit", "forceQuit"]:
            assert expected_op in app_ops, f"LLAppViewer missing op '{expected_op}'"

        results["test_1_4_get_api"] = {"status": "PASS", "duration": time.time() - t0}
        print("  [PASS] 1.4 Introspection getAPI (LLViewerControl & LLAppViewer)")
    except Exception as err:
        results["test_1_4_get_api"] = {"status": "FAIL", "error": str(err)}
        print(f"  [FAIL] 1.4 Introspection getAPI: {err}")

    # 1.5 LLViewerControl Setting Manipulation (get, set, toggle, groups, vars)
    t0 = time.time()
    try:
        # DrawDistance (Real)
        init_dd = client.request("LLViewerControl", op="get", data={"key": "DrawDistance"}, reqid=105)
        orig_dd = float(init_dd.get("value", 128.0))

        set_dd = client.request("LLViewerControl", op="set", data={"key": "DrawDistance", "value": 256.0}, reqid=106)
        assert set_dd.get("status") is True
        assert abs(float(set_dd.get("value")) - 256.0) < 1e-3

        verify_dd = client.request("LLViewerControl", op="get", data={"key": "DrawDistance"}, reqid=107)
        assert abs(float(verify_dd.get("value")) - 256.0) < 1e-3

        # Restore DrawDistance
        client.request("LLViewerControl", op="set", data={"key": "DrawDistance", "value": orig_dd}, reqid=108)

        # VSync (Boolean Toggle)
        init_vsync = client.request("LLViewerControl", op="get", data={"key": "VSync"}, reqid=109)
        orig_vsync = bool(init_vsync.get("value", False))

        toggled_vsync = client.request("LLViewerControl", op="toggle", data={"key": "VSync"}, reqid=110)
        assert toggled_vsync.get("status") is True
        assert bool(toggled_vsync.get("value")) == (not orig_vsync)

        # Restore VSync
        client.request("LLViewerControl", op="toggle", data={"key": "VSync"}, reqid=111)

        # Shadows (Boolean Toggle)
        init_shad = client.request("LLViewerControl", op="get", data={"key": "Shadows"}, reqid=112)
        orig_shad = bool(init_shad.get("value", False))
        toggled_shad = client.request("LLViewerControl", op="toggle", data={"key": "Shadows"}, reqid=113)
        assert bool(toggled_shad.get("value")) == (not orig_shad)
        client.request("LLViewerControl", op="toggle", data={"key": "Shadows"}, reqid=114)

        # Language (String Mutation)
        init_lang = client.request("LLViewerControl", op="get", data={"key": "Language"}, reqid=115)
        orig_lang = str(init_lang.get("value", "default"))
        set_lang = client.request("LLViewerControl", op="set", data={"key": "Language", "value": "fr"}, reqid=116)
        assert set_lang.get("value") == "fr"
        # Restore Language
        client.request("LLViewerControl", op="set", data={"key": "Language", "value": orig_lang}, reqid=117)

        # Groups enumeration
        groups_resp = client.request("LLViewerControl", op="groups", reqid=118)
        groups = groups_resp.get("groups", [])
        assert "Display" in groups
        assert "Graphics" in groups

        # Vars enumeration
        vars_resp = client.request("LLViewerControl", op="vars", reqid=119)
        vars_map = vars_resp.get("vars", {})
        assert "DrawDistance" in vars_map
        assert "VSync" in vars_map
        assert "Shadows" in vars_map

        results["test_1_5_viewer_control"] = {"status": "PASS", "duration": time.time() - t0}
        print("  [PASS] 1.5 LLViewerControl (get, set, toggle, groups, vars)")
    except Exception as err:
        results["test_1_5_viewer_control"] = {"status": "FAIL", "error": str(err)}
        print(f"  [FAIL] 1.5 LLViewerControl: {err}")

    # 1.6 Subsystem Status and Telemetry Queries
    t0 = time.time()
    try:
        # LLAppViewer.status
        app_status = client.request("LLAppViewer", op="status", reqid=120)
        assert "version" in app_status, f"Missing version in status: {app_status}"
        assert "uptime" in app_status
        assert "frame_count" in app_status
        assert app_status.get("screen") == "Login"

        # LLAgent.getPosition
        agent_pos = client.request("LLAgent", op="getPosition", reqid=121)
        assert "position" in agent_pos
        assert "x" in agent_pos and "y" in agent_pos and "z" in agent_pos

        # LLAgent.getID
        agent_id = client.request("LLAgent", op="getID", reqid=122)
        assert "id" in agent_id
        assert "agent_id" in agent_id

        # LLLogin.getState
        login_state = client.request("LLLogin", op="getState", reqid=123)
        assert login_state.get("state") == "STATE_LOGIN_SHOW"
        assert login_state.get("logged_in") is False

        # LLCamera.get
        cam_info = client.request("LLCamera", op="get", reqid=124)
        assert "position" in cam_info
        assert "target" in cam_info
        assert "fov" in cam_info

        # LLInventory.getRoot
        inv_root = client.request("LLInventory", op="getRoot", reqid=125)
        assert "root" in inv_root
        assert "owner" in inv_root

        results["test_1_6_subsystems"] = {"status": "PASS", "duration": time.time() - t0}
        print("  [PASS] 1.6 Subsystems (LLAppViewer, LLAgent, LLLogin, LLCamera, LLInventory)")
    except Exception as err:
        results["test_1_6_subsystems"] = {"status": "FAIL", "error": str(err)}
        print(f"  [FAIL] 1.6 Subsystems: {err}")

    return results


if __name__ == "__main__":
    c = LeapClient()
    try:
        # Initial ping to await first frame
        c.ping(timeout=30.0)
        res = run_tier1_tests(c)
        all_passed = all(r.get("status") == "PASS" for r in res.values())
        print(f"\nTier 1 Summary: {'ALL PASS' if all_passed else 'FAILURES DETECTED'}")
        c.quit_gracefully(timeout=5.0)
        sys.exit(0 if all_passed else 1)
    finally:
        if c.is_alive():
            c.terminate()
