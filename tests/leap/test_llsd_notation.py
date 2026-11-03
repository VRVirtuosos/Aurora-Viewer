"""Unit tests for llsd_notation.py."""

import sys
from pathlib import Path

# Ensure tests/leap is in python path
sys.path.insert(0, str(Path(__file__).parent))

from llsd_notation import format_notation, parse_notation, UNDEF


def test_roundtrip_primitives():
    # None / Undef
    assert parse_notation(format_notation(None)) is None
    assert parse_notation(format_notation(UNDEF)) is None

    # Booleans
    assert parse_notation(format_notation(True)) is True
    assert parse_notation(format_notation(False)) is False

    # Integers
    assert parse_notation(format_notation(0)) == 0
    assert parse_notation(format_notation(42)) == 42
    assert parse_notation(format_notation(-999)) == -999

    # Floats / Reals
    assert abs(parse_notation(format_notation(3.14159)) - 3.14159) < 1e-5
    assert abs(parse_notation(format_notation(-0.001)) - (-0.001)) < 1e-5

    # Strings
    assert parse_notation(format_notation("hello world")) == "hello world"
    assert parse_notation(format_notation("newline\ntab\tquote'slash\\")) == "newline\ntab\tquote'slash\\"

    # Binary
    blob = b"\x00\x01\x02\xff\xfe"
    assert parse_notation(format_notation(blob)) == blob


def test_complex_structures():
    data = {
        "command": "21f50d6c-7d34-4dff-b943-65c889fabd3a",
        "features": {},
        "nested": {
            "arr": [1, 2, 3, "four", False, None],
            "real": 128.5,
        },
    }
    encoded = format_notation(data)
    decoded = parse_notation(encoded)
    assert decoded["command"] == data["command"]
    assert decoded["nested"]["arr"] == [1, 2, 3, "four", False, None]
    assert abs(decoded["nested"]["real"] - 128.5) < 1e-5


def test_aurora_native_tokens():
    # Test parsing tokens emitted by aurora-viewer / aurora-llsd
    sample = "{'pump':u367e49a8-00e2-4efc-af63-2cdf6e6812a9,'data':{'command':'cmd-123','reqid':i101,'fps':r60.0,'flag':1,'off':0}}"
    parsed = parse_notation(sample)
    assert parsed["pump"] == "367e49a8-00e2-4efc-af63-2cdf6e6812a9"
    assert parsed["data"]["command"] == "cmd-123"
    assert parsed["data"]["reqid"] == 101
    assert parsed["data"]["fps"] == 60.0
    assert parsed["data"]["flag"] is True
    assert parsed["data"]["off"] is False


def test_aurora_intro_packet():
    intro_payload = (
        b"{'data':{'command':'21f50d6c-7d34-4dff-b943-65c889fabd3a','features':{}},"
        b"'pump':'367e49a8-00e2-4efc-af63-2cdf6e6812a9'}"
    )
    parsed = parse_notation(intro_payload)
    assert parsed["pump"] == "367e49a8-00e2-4efc-af63-2cdf6e6812a9"
    assert parsed["data"]["command"] == "21f50d6c-7d34-4dff-b943-65c889fabd3a"
    assert parsed["data"]["features"] == {}


if __name__ == "__main__":
    test_roundtrip_primitives()
    test_complex_structures()
    test_aurora_native_tokens()
    test_aurora_intro_packet()
    print("ALL LLSD NOTATION TESTS PASSED!")
