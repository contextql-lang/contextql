"""Extended MCP/REMOTE provider contracts (plan section 8.2, PR 11)."""
import pandas as pd
import pytest

import contextql as cql
from contextql.providers import MCPResult, RemoteResult


def roaring_payload(ids):
    pytest.importorskip("pyroaring")
    from pyroaring import BitMap64
    return BitMap64(ids).serialize()


class TestMCPResultContract:
    def test_id_list_form(self):
        result = MCPResult(entity_type="t", entity_ids=[1, 2, 3])
        assert list(result.membership_array()) == [1, 2, 3]

    def test_bitmap_form(self):
        result = MCPResult(
            entity_type="t",
            membership_bitmap=roaring_payload([5, 9, 2**40]),
            bitmap_encoding="roaring64",
        )
        assert sorted(result.membership_array()) == [5, 9, 2**40]

    def test_both_forms_rejected(self):
        with pytest.raises(ValueError, match="exactly one"):
            MCPResult(
                entity_type="t",
                entity_ids=[1],
                membership_bitmap=b"x",
                bitmap_encoding="roaring64",
            )

    def test_neither_form_rejected(self):
        with pytest.raises(ValueError, match="exactly one"):
            MCPResult(entity_type="t")

    def test_bitmap_requires_encoding(self):
        with pytest.raises(ValueError, match="bitmap_encoding"):
            MCPResult(entity_type="t", membership_bitmap=b"x")

    def test_unknown_encoding_rejected_on_decode(self):
        result = MCPResult(
            entity_type="t", membership_bitmap=b"x", bitmap_encoding="cbor"
        )
        with pytest.raises(ValueError, match="encoding"):
            result.membership_array()

    def test_malformed_payload_rejected(self):
        pytest.importorskip("pyroaring")
        result = MCPResult(
            entity_type="t",
            membership_bitmap=b"not a bitmap",
            bitmap_encoding="roaring64",
        )
        with pytest.raises(ValueError, match="[Mm]alformed"):
            result.membership_array()

    def test_oversized_payload_rejected(self):
        from contextql.providers.base import MAX_BITMAP_PAYLOAD_BYTES
        result = MCPResult(
            entity_type="t",
            membership_bitmap=b"\x00" * (MAX_BITMAP_PAYLOAD_BYTES + 1),
            bitmap_encoding="roaring64",
        )
        with pytest.raises(ValueError, match="exceeds"):
            result.membership_array()

    def test_score_map_from_parallel_lists(self):
        result = MCPResult(
            entity_type="t", entity_ids=[1, 2], scores=[0.9, 0.4]
        )
        assert result.score_map() == {1: 0.9, 2: 0.4}

    def test_score_map_from_dict(self):
        result = MCPResult(
            entity_type="t",
            membership_bitmap=roaring_payload([1, 2]),
            bitmap_encoding="roaring64",
            scores={1: 0.9, 2: 0.4},
        )
        assert result.score_map() == {1: 0.9, 2: 0.4}

    def test_metadata_fields(self):
        result = MCPResult(
            entity_type="t",
            entity_ids=[1],
            entity_key_type="INT64",
            data_as_of="2026-07-23T12:00:00Z",
            source_watermark="wm-17",
            evidence_refs={1: "doc://case/1"},
            next_cursor="page-2",
        )
        assert result.entity_key_type == "INT64"
        assert result.next_cursor == "page-2"


class TestRemoteResultContract:
    def test_metadata_fields(self):
        result = RemoteResult(
            rows=[{"a": 1}],
            schema={"a": "INT64"},
            data_as_of="2026-07-23T12:00:00Z",
            source_watermark="wm-3",
            next_cursor=None,
        )
        assert result.schema == {"a": "INT64"}
        assert result.to_dataframe().iloc[0]["a"] == 1


class TestExecutorWithBitmapMCP:
    def test_bitmap_mcp_provider_end_to_end(self):
        pytest.importorskip("pyroaring")

        class BitmapProvider:
            def resolve(self, entity_type, params, limit=None):
                return MCPResult(
                    entity_type=entity_type,
                    membership_bitmap=roaring_payload([2, 4]),
                    bitmap_encoding="roaring64",
                    scores={2: 0.8, 4: 0.6},
                    data_as_of="2026-07-23T12:00:00Z",
                )

        engine = cql.Engine()
        engine.register_table(
            "txns",
            pd.DataFrame({"txn_id": [1, 2, 3, 4], "amount": [10, 20, 30, 40]}),
            primary_key="txn_id",
        )
        engine.register_mcp_provider("risk_model", BitmapProvider())
        result = engine.execute(
            "SELECT txn_id, CONTEXT_SCORE() AS s FROM txns "
            "WHERE CONTEXT IN (MCP(risk_model)) ORDER BY CONTEXT DESC;"
        )
        df = result.to_pandas()
        assert list(df["txn_id"]) == [2, 4]
        assert list(df["s"]) == [pytest.approx(0.8), pytest.approx(0.6)]
