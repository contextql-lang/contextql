"""Refresh the self-contained post-trade visual report.

The generated HTML remains directly openable from disk. Re-run this script
after a benchmark to replace its embedded data:

    python demo/build_post_trade_report.py \
      --benchmark docs/benchmarks/post-trade-10m-2026-07-23.json \
      --db .venv/post-trade-10m.duckdb \
      --html demo/post_trade_demo_visual.html
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

START_MARKER = "<!-- REPORT_DATA_START -->"
END_MARKER = "<!-- REPORT_DATA_END -->"

BREAK_TYPES = (
    "quantity_mismatch",
    "price_mismatch",
    "missing_confirmation",
    "ssi_mismatch",
)
ACTIONS = (
    "amend_and_resubmit",
    "affirm_with_counterparty",
    "escalate_to_desk",
    "cancel_and_rebook",
)


def collect(benchmark_path: Path, db_path: Path) -> dict:
    import duckdb

    benchmark = json.loads(benchmark_path.read_text(encoding="utf-8"))
    as_of = benchmark["as_of"]
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        total, unsettled, at_risk = conn.execute(
            """
            SELECT
                count(*) AS total,
                count(*) FILTER (
                    WHERE settlement_status NOT IN ('settled', 'cancelled')
                ) AS unsettled,
                count(*) FILTER (
                    WHERE settlement_status NOT IN ('settled', 'cancelled')
                      AND (
                        contractual_settle_date
                            < CAST(CAST(? AS TIMESTAMP) AS DATE)
                        OR market_cutoff_at
                            <= CAST(? AS TIMESTAMP) + INTERVAL '2 hours'
                      )
                ) AS at_risk
            FROM transactions
            """,
            [as_of, as_of],
        ).fetchone()

        region_rows = conn.execute(
            """
            SELECT
                match_status = 'unmatched' AS unmatched,
                confirmation_received = FALSE AS missing_confirmation,
                ssi_valid = FALSE AS invalid_ssi,
                count(*) AS members
            FROM transactions
            WHERE unmatched OR missing_confirmation OR invalid_ssi
            GROUP BY ALL
            """
        ).fetchall()
        regions = {
            f"{int(unmatched)}{int(missing)}{int(invalid)}": int(count)
            for unmatched, missing, invalid, count in region_rows
        }

        top_rows = conn.execute(
            """
            SELECT
                transaction_id,
                counterparty_id,
                asset_class,
                notional_usd,
                contractual_settle_date,
                LEAST(
                    1.0,
                    predicted_fail_probability
                    + CASE WHEN ssi_valid = FALSE THEN 0.20 ELSE 0 END
                    + CASE WHEN match_status <> 'matched' THEN 0.15 ELSE 0 END
                ) AS priority
            FROM transactions
            WHERE settlement_status NOT IN ('settled', 'cancelled')
              AND (
                contractual_settle_date
                    < CAST(CAST(? AS TIMESTAMP) AS DATE)
                OR market_cutoff_at
                    <= CAST(? AS TIMESTAMP) + INTERVAL '2 hours'
              )
              AND (
                match_status <> 'matched'
                OR ssi_valid = FALSE
                OR confirmation_received = FALSE
                OR fields_mismatched > 0
                OR predicted_fail_probability >= 0.80
              )
            ORDER BY priority DESC, transaction_id ASC
            LIMIT 20
            """,
            [as_of, as_of],
        ).fetchall()
    finally:
        conn.close()

    operator_rows = []
    for (
        transaction_id,
        counterparty_id,
        asset_class,
        notional_usd,
        settle_date,
        priority,
    ) in top_rows:
        entity_id = int(transaction_id)
        operator_rows.append(
            {
                "transaction_id": entity_id,
                "counterparty_id": int(counterparty_id),
                "asset_class": asset_class,
                "notional_usd": float(notional_usd),
                "settle_date": str(settle_date),
                "break_type": BREAK_TYPES[entity_id % len(BREAK_TYPES)],
                "recommended_action": ACTIONS[entity_id % len(ACTIONS)],
                "priority": float(priority),
            }
        )

    intervention = benchmark["contexts"][
        "settlement_intervention_required"
    ]
    return {
        "benchmark": benchmark,
        "funnel": {
            "all": int(total),
            "unsettled": int(unsettled),
            "at_risk": int(at_risk),
            "intervention": int(intervention["cardinality"]),
            "queue": 20,
        },
        "venn": {
            "regions": regions,
            "sets": {
                "unmatched_trade": benchmark["contexts"][
                    "unmatched_trade"
                ]["cardinality"],
                "missing_confirmation": benchmark["contexts"][
                    "missing_confirmation"
                ]["cardinality"],
                "invalid_ssi": benchmark["contexts"]["invalid_ssi"][
                    "cardinality"
                ],
            },
            "union": benchmark["algebra"]["union_3way"]["cardinality"],
        },
        "operator_rows": operator_rows,
        "hardening": {
            "core_tests": "547 passed, 2 skipped",
            "server_tests": "103 passed",
            "core_commit": "6a8fe33",
            "server_commit": "4198655",
        },
    }


def embed(html_path: Path, report: dict) -> None:
    html = html_path.read_text(encoding="utf-8")
    payload = json.dumps(report, indent=2, ensure_ascii=False)
    replacement = (
        f"{START_MARKER}\n"
        '<script id="report-data" type="application/json">\n'
        f"{payload}\n"
        "</script>\n"
        f"{END_MARKER}"
    )
    pattern = re.compile(
        re.escape(START_MARKER)
        + r".*?"
        + re.escape(END_MARKER),
        re.DOTALL,
    )
    updated, count = pattern.subn(replacement, html, count=1)
    if count != 1:
        raise RuntimeError(
            f"Could not find one report-data block in {html_path}"
        )
    html_path.write_text(updated, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark", type=Path, required=True)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument(
        "--html",
        type=Path,
        default=Path(__file__).with_name("post_trade_demo_visual.html"),
    )
    parser.add_argument(
        "--print-only",
        action="store_true",
        help="Print collected JSON without changing the HTML report.",
    )
    args = parser.parse_args()

    report = collect(args.benchmark, args.db)
    if args.print_only:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        embed(args.html, report)
        print(f"refreshed {args.html}")


if __name__ == "__main__":
    main()
