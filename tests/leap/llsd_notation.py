"""
Pure Python LLSD Notation Serializer and Stateful Parser.

Implements serialization and deserialization for the LLSD Notation format
(application/llsd+notation) as defined by Linden Lab LEAP and Aurora-Viewer.
Zero third-party dependencies.
"""

import base64
import re
from typing import Any, Dict, List, Optional, Tuple, Union


class LlsdUndef:
    """Represents LLSD Undefined ('!')."""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "LlsdUndef"

    def __bool__(self):
        return False


UNDEF = LlsdUndef()


def format_notation(val: Any) -> str:
    """Serialize a Python data structure into LLSD Notation format."""
    if val is None or val is UNDEF:
        return "!"

    # bool must be checked before int because bool subclasses int in Python
    if isinstance(val, bool):
        return "1" if val else "0"

    if isinstance(val, int):
        return f"i{val}"

    if isinstance(val, float):
        return f"r{val}"

    if isinstance(val, str):
        escaped = (
            val.replace("\\", "\\\\")
               .replace("'", "\\'")
               .replace("\n", "\\n")
               .replace("\r", "\\r")
               .replace("\t", "\\t")
        )
        return f"'{escaped}'"

    if isinstance(val, (bytes, bytearray)):
        b64 = base64.b64encode(val).decode("ascii")
        return f'b64"{b64}"'

    if isinstance(val, (list, tuple)):
        elems = [format_notation(item) for item in val]
        return "[" + ",".join(elems) + "]"

    if isinstance(val, dict):
        pairs = [f"{format_notation(str(k))}:{format_notation(v)}" for k, v in val.items()]
        return "{" + ",".join(pairs) + "}"

    # Fallback to string representation
    return format_notation(str(val))


class LlsdParser:
    """Recursive-descent parser for LLSD Notation bytes."""

    def __init__(self, data: Union[bytes, str]):
        if isinstance(data, str):
            self.data = data.encode("utf-8")
        else:
            self.data = bytes(data)
        self.pos = 0
        self.len = len(self.data)

    def peek(self) -> Optional[int]:
        if self.pos < self.len:
            return self.data[self.pos]
        return None

    def next(self) -> int:
        c = self.peek()
        if c is None:
            raise EOFError("Unexpected end of LLSD notation buffer")
        self.pos += 1
        return c

    def skip_space(self) -> None:
        while self.pos < self.len:
            c = self.data[self.pos]
            if c in (32, 9, 10, 13, ord(",")):  # space, \t, \n, \r, comma
                self.pos += 1
            else:
                break

    def parse(self) -> Any:
        self.skip_space()
        val = self._parse_value(depth=0)
        return val

    def _parse_value(self, depth: int = 0) -> Any:
        if depth > 128:
            raise ValueError("LLSD notation recursion limit exceeded")

        self.skip_space()
        c = self.peek()
        if c is None:
            raise EOFError("Unexpected end of LLSD notation data")

        # Map: '{'
        if c == ord("{"):
            return self._parse_map(depth)

        # Array: '['
        if c == ord("["):
            return self._parse_array(depth)

        # Undef: '!'
        if c == ord("!"):
            self.pos += 1
            return None

        # Boolean: '0' or '1'
        if c == ord("0"):
            self.pos += 1
            return False
        if c == ord("1"):
            self.pos += 1
            return True

        # Words: boolean 'true' / 'false' / 't' / 'f'
        if chr(c) in "tTfF":
            word = self._take_word()
            w_lower = word.lower()
            if w_lower in ("t", "true"):
                return True
            if w_lower in ("f", "false"):
                return False
            raise ValueError(f"Unrecognized boolean token '{word}'")

        # Integer: 'i'
        if c == ord("i"):
            self.pos += 1
            num_str = self._take_digits_signed()
            return int(num_str)

        # Real: 'r'
        if c == ord("r"):
            self.pos += 1
            num_str = self._take_float_chars()
            return float(num_str)

        # UUID: 'u'
        if c == ord("u"):
            self.pos += 1
            uuid_bytes = self.data[self.pos : self.pos + 36]
            self.pos += 36
            return uuid_bytes.decode("ascii")

        # String: "'" or '"'
        if c in (ord("'"), ord('"')):
            return self._parse_string()

        # Sized string: 's'
        if c == ord("s"):
            self.pos += 1
            return self._parse_sized_string()

        # URI: 'l'
        if c == ord("l"):
            self.pos += 1
            return self._parse_string()

        # Date: 'd'
        if c == ord("d"):
            self.pos += 1
            return self._parse_string()

        # Binary: 'b'
        if c == ord("b"):
            self.pos += 1
            return self._parse_binary()

        # Permissive fallback: bare numbers without 'i' or 'r' prefix
        if c in (ord("-"), ord("+")) or (ord("0") <= c <= ord("9")):
            num_str = self._take_float_chars()
            if "." in num_str or "e" in num_str.lower():
                return float(num_str)
            return int(num_str)

        raise ValueError(f"Unexpected token byte {c} ({chr(c)!r}) at position {self.pos}")

    def _parse_map(self, depth: int) -> Dict[str, Any]:
        self.pos += 1  # consume '{'
        res: Dict[str, Any] = {}
        while True:
            self.skip_space()
            c = self.peek()
            if c is None:
                raise EOFError("Unterminated LLSD map: missing '}'")
            if c == ord("}"):
                self.pos += 1
                return res

            # Parse key (can be string or sized string)
            if c in (ord("'"), ord('"')):
                key = self._parse_string()
            elif c == ord("s"):
                self.pos += 1
                key = self._parse_sized_string()
            else:
                # Bareword or identifier key
                key = self._take_key_name()

            # Skip colon or whitespace between key and value
            while self.pos < self.len and self.data[self.pos] in (32, 9, 10, 13, ord(":")):
                self.pos += 1

            val = self._parse_value(depth + 1)
            res[key] = val

            # Skip optional separator
            self.skip_space()

    def _parse_array(self, depth: int) -> List[Any]:
        self.pos += 1  # consume '['
        res: List[Any] = []
        while True:
            self.skip_space()
            c = self.peek()
            if c is None:
                raise EOFError("Unterminated LLSD array: missing ']'")
            if c == ord("]"):
                self.pos += 1
                return res

            val = self._parse_value(depth + 1)
            res.append(val)
            self.skip_space()

    def _parse_string(self) -> str:
        delim = self.next()
        chars: List[str] = []
        while True:
            c = self.next()
            if c == delim:
                break
            if c == ord("\\"):
                nxt = self.next()
                if nxt == ord("n"):
                    chars.append("\n")
                elif nxt == ord("r"):
                    chars.append("\r")
                elif nxt == ord("t"):
                    chars.append("\t")
                elif nxt == ord("\\"):
                    chars.append("\\")
                elif nxt == delim:
                    chars.append(chr(delim))
                elif nxt == ord("x"):
                    h = self.data[self.pos : self.pos + 2].decode("ascii")
                    self.pos += 2
                    chars.append(chr(int(h, 16)))
                else:
                    chars.append(chr(nxt))
            else:
                # Multi-byte UTF-8 handling
                if c < 0x80:
                    chars.append(chr(c))
                else:
                    # Collect full utf-8 sequence
                    byte_seq = bytearray([c])
                    if (c & 0xE0) == 0xC0:
                        byte_seq.append(self.next())
                    elif (c & 0xF0) == 0xE0:
                        byte_seq.extend([self.next(), self.next()])
                    elif (c & 0xF8) == 0xF0:
                        byte_seq.extend([self.next(), self.next(), self.next()])
                    chars.append(byte_seq.decode("utf-8", errors="replace"))

        return "".join(chars)

    def _parse_sized_string(self) -> str:
        if self.next() != ord("("):
            raise ValueError("Expected '(' for sized string")
        n_str = ""
        while True:
            c = self.next()
            if c == ord(")"):
                break
            n_str += chr(c)
        sz = int(n_str.strip())
        quote = self.next()
        raw = self.data[self.pos : self.pos + sz]
        self.pos += sz
        closing = self.next()
        if closing != quote:
            raise ValueError("Mismatched quote on sized string")
        return raw.decode("utf-8", errors="replace")

    def _parse_binary(self) -> bytes:
        # Check for '64"' (base64) or '(len)"' (raw)
        if self.data[self.pos : self.pos + 3] == b'64"':
            self.pos += 3
            end_quote = self.data.find(b'"', self.pos)
            if end_quote == -1:
                raise EOFError("Unterminated b64 binary")
            b64_str = self.data[self.pos : end_quote]
            self.pos = end_quote + 1
            return base64.b64decode(b64_str)
        if self.peek() == ord("("):
            self.pos += 1
            n_str = ""
            while True:
                c = self.next()
                if c == ord(")"):
                    break
                n_str += chr(c)
            sz = int(n_str.strip())
            quote = self.next()
            raw = self.data[self.pos : self.pos + sz]
            self.pos += sz
            self.next()  # closing quote
            return raw
        raise ValueError("Invalid binary format in notation")

    def _take_word(self) -> str:
        start = self.pos
        while self.pos < self.len and chr(self.data[self.pos]).isalpha():
            self.pos += 1
        return self.data[start : self.pos].decode("ascii")

    def _take_digits_signed(self) -> str:
        start = self.pos
        while self.pos < self.len and (chr(self.data[self.pos]).isdigit() or self.data[self.pos] in (ord("-"), ord("+"))):
            self.pos += 1
        return self.data[start : self.pos].decode("ascii")

    def _take_float_chars(self) -> str:
        start = self.pos
        while self.pos < self.len:
            c = self.data[self.pos]
            if chr(c).isdigit() or c in (ord("-"), ord("+"), ord("."), ord("e"), ord("E")):
                self.pos += 1
            else:
                break
        return self.data[start : self.pos].decode("ascii")

    def _take_key_name(self) -> str:
        start = self.pos
        while self.pos < self.len:
            c = self.data[self.pos]
            if chr(c).isalnum() or c in (ord("_"), ord("-"), ord(".")):
                self.pos += 1
            else:
                break
        return self.data[start : self.pos].decode("ascii")


def parse_notation(data: Union[bytes, str]) -> Any:
    """Parse LLSD Notation string or bytes into Python objects."""
    return LlsdParser(data).parse()
