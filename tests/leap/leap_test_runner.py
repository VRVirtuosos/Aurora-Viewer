"""
Master Automated Test Runner for Aurora-Viewer Native Rust LEAP Protocol.

Executes all 4 opaque-box integration test tiers against the compiled
Aurora-Viewer executable (`target/debug/aurora-viewer.exe --leap-stdio`) as well as
foundational protocol codec unit tests.

Test Suite Structure:
  - Unit Tests:
      * LLSD Notation Parser & Serializer (`test_llsd_notation.py`)
      * Length-Prefix Stream Framing Codec (`test_leap_codec.py`)
  - Integration Tiers:
      * Tier 1: Feature Coverage (Handshake, Ping/Pong, Introspection, Settings, Subsystems)
      * Tier 2: Boundary & Corner Cases (Byte streaming, 100-500KB payloads, Complex reqid, Errors)
      * Tier 3: Cross-Feature & Pub/Sub (Listener lifecycle, Chat forwarding, 75 interleaved pipeline)
      * Tier 4: Real-World Workloads (E2E automation scenario, Graceful quit, Multi-session lifecycle)
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

LEAP_DIR = Path(__file__).parent.resolve()
VIEWER_DIR = LEAP_DIR.parent.parent.resolve()
EXE_PATH = VIEWER_DIR / "target" / "debug" / "aurora-viewer.exe"


def run_subtest(name: str, py_file: Path) -> Tuple[bool, float, str]:
    """Run a Python test module and capture outcome."""
    cmd = [sys.executable, "-u", str(py_file)]
    t0 = time.time()
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(VIEWER_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=120,
        )
        duration = time.time() - t0
        passed = (proc.returncode == 0)
        output = proc.stdout.strip()
        return passed, duration, output
    except subprocess.TimeoutExpired as e:
        duration = time.time() - t0
        return False, duration, f"TIMEOUT after 120s:\n{e.stdout}"
    except Exception as e:
        duration = time.time() - t0
        return False, duration, f"EXECUTION ERROR: {e}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Aurora-Viewer LEAP Automated Test Runner")
    parser.add_argument(
        "--tier",
        type=str,
        default="all",
        help="Specify tier to run (e.g., '1', '2', '3', '4', 'unit', or 'all')",
    )
    args = parser.parse_args()

    print("=" * 80)
    print("AURORA-VIEWER LEAP AUTOMATED TEST SUITE")
    print(f"Target Binary: {EXE_PATH}")
    print(f"Binary Present: {EXE_PATH.exists()}")
    print("=" * 80)

    if not EXE_PATH.exists():
        print(f"FATAL: Target executable not found at {EXE_PATH}")
        return 1

    test_registry: List[Tuple[str, str, Path]] = [
        ("Unit - LLSD Notation", "unit", LEAP_DIR / "test_llsd_notation.py"),
        ("Unit - Framing Codec", "unit", LEAP_DIR / "test_leap_codec.py"),
        ("Tier 1 - Feature Coverage", "1", LEAP_DIR / "test_tier1_features.py"),
        ("Tier 2 - Boundary & Corner Cases", "2", LEAP_DIR / "test_tier2_boundaries.py"),
        ("Tier 3 - Cross-Feature & Pub/Sub", "3", LEAP_DIR / "test_tier3_pubsub.py"),
        ("Tier 4 - Real-World Workloads", "4", LEAP_DIR / "test_tier4_workloads.py"),
    ]

    selected_tests = []
    for label, tier_tag, path in test_registry:
        if args.tier.lower() == "all" or args.tier.lower() == tier_tag:
            selected_tests.append((label, tier_tag, path))

    results = []
    total_t0 = time.time()

    for label, tier_tag, path in selected_tests:
        print(f"\n[RUNNING] {label} ({path.name})...")
        passed, duration, output = run_subtest(label, path)
        status_str = "PASS" if passed else "FAIL"
        results.append((label, path.name, status_str, duration, output))
        print(f"  -> Result: {status_str} ({duration:.2f}s)")
        if not passed:
            print("  --- Output Snippet ---")
            for line in output.splitlines()[-20:]:
                print(f"    {line}")
            print("  ----------------------")

    total_duration = time.time() - total_t0

    # Summary Table
    print("\n" + "=" * 80)
    print("LEAP TEST EXECUTION SUMMARY")
    print("=" * 80)
    print(f"{'Test Suite':<36} | {'Script':<25} | {'Status':<6} | {'Time (s)':<8}")
    print("-" * 80)

    all_passed = True
    for label, script, status, duration, _ in results:
        if status != "PASS":
            all_passed = False
        print(f"{label:<36} | {script:<25} | {status:<6} | {duration:>7.2f}s")

    print("-" * 80)
    print(f"Total Suites Run: {len(results)}")
    print(f"Overall Status:   {'ALL PASS' if all_passed else 'FAILURES DETECTED'}")
    print(f"Total Time:       {total_duration:.2f}s")
    print("=" * 80)

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
