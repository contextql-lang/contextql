"""Versioned binary score encoding used by persistent snapshot stores."""
from __future__ import annotations

import struct
from typing import Mapping

_SCORE_MAGIC = b"CQLS"
_SCORE_VERSION = 1
_SCORE_ENTRY = struct.Struct("<Qd")


def encode_scores(scores: Mapping[int, float]) -> bytes:
    ordered = sorted((int(key), float(value)) for key, value in scores.items())
    body = b"".join(_SCORE_ENTRY.pack(key, value) for key, value in ordered)
    return (
        _SCORE_MAGIC
        + struct.pack("<BQ", _SCORE_VERSION, len(ordered))
        + body
    )


def decode_scores(payload: bytes | None) -> dict[int, float]:
    if not payload:
        return {}
    if len(payload) < 13 or not payload.startswith(_SCORE_MAGIC):
        raise ValueError("[E201] malformed score payload header.")
    version, count = struct.unpack_from("<BQ", payload, 4)
    if version != _SCORE_VERSION:
        raise ValueError(
            f"[E201] unsupported score payload version {version}."
        )
    body = payload[13:]
    expected = count * _SCORE_ENTRY.size
    if len(body) != expected:
        raise ValueError(
            f"[E201] score payload expected {expected} bytes, "
            f"received {len(body)}."
        )
    return {
        entity_id: score
        for entity_id, score in _SCORE_ENTRY.iter_unpack(body)
    }
