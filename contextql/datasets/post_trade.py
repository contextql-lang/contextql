"""Deterministic post-trade transaction dataset (plan section 9).

Generates a canonical post-trade transaction table directly inside DuckDB —
no Python objects, no Pandas materialization (plan 9.1). All randomness is
derived from ``hash()`` of the row index with per-column salts, so the data
is fully deterministic for a given ``(row_count, seed, as_of)`` regardless of
DuckDB's RNG implementation.

Causal structure (plan 9.2):

- Invalid/stale SSIs raise settlement-failure probability.
- Missing confirmations correlate with economic-term breaks.
- Failures cluster on ~5% of counterparties and selected markets.
- Cross-border trades settle T+3 with more cutoff exposure.
- High-notional trades are uncommon (heavy-tailed notionals).
- Manual touches grow with exception age and mismatch count.
- Most transactions are clean; exceptions are selective enough for
  bitmap pushdown to matter.
"""
from __future__ import annotations

from datetime import datetime

DEFAULT_AS_OF = "2026-07-23 12:00:00"


def _u(salt: int) -> str:
    """Uniform [0, 1) derived from the row index — deterministic."""
    return f"((hash(i * 2654435761 + {salt}) % 1000000) + 1000000) % 1000000 / 1000000.0"


def generate_post_trade_dataset(
    conn,
    row_count: int = 10_000_000,
    *,
    table_name: str = "transactions",
    seed: int = 42,
    as_of: str = DEFAULT_AS_OF,
) -> None:
    """Create *table_name* in *conn* with *row_count* deterministic rows.

    ``as_of`` anchors every time-derived column so runs are reproducible;
    pass the same value to the reference context SQL.
    """
    sql = _generation_sql(row_count, table_name, seed, as_of)
    conn.execute(sql)


def _generation_sql(
    row_count: int, table_name: str, seed: int, as_of: str
) -> str:
    s = seed * 1_000_003
    u = lambda k: _u(s + k)  # noqa: E731 - local shorthand for column salts
    return f"""
CREATE OR REPLACE TABLE {table_name} AS
WITH base AS (
    SELECT
        i,
        TIMESTAMP '{as_of}' AS as_of,
        {u(1)} AS u_cp,
        {u(2)} AS u_market,
        {u(3)} AS u_asset,
        {u(4)} AS u_age,
        {u(5)} AS u_notional,
        {u(6)} AS u_match,
        {u(7)} AS u_ssi,
        {u(8)} AS u_conf,
        {u(9)} AS u_fail,
        {u(10)} AS u_noise,
        {u(11)} AS u_side,
        {u(12)} AS u_price,
        {u(13)} AS u_qty,
        {u(14)} AS u_cutoff,
        {u(15)} AS u_break,
        {u(16)} AS u_docs,
        {u(17)} AS u_anom,
        {u(18)} AS u_quality,
        {u(19)} AS u_model
    FROM generate_series(0, {row_count - 1}) AS t(i)
),
entities AS (
    SELECT
        *,
        1 + CAST(u_cp * 2000 AS BIGINT) AS counterparty_id,
        -- ~5% of counterparties carry clustered operational risk
        (hash(1 + CAST(u_cp * 2000 AS BIGINT)) % 100) < 5 AS cp_risky,
        CASE
            WHEN u_market < 0.45 THEN 'US'
            WHEN u_market < 0.70 THEN 'EU'
            WHEN u_market < 0.85 THEN 'UK'
            WHEN u_market < 0.95 THEN 'JP'
            ELSE 'HK'
        END AS market,
        u_market >= 0.45 AS cross_border,
        CASE
            WHEN u_asset < 0.50 THEN 'equity'
            WHEN u_asset < 0.75 THEN 'fixed_income'
            WHEN u_asset < 0.92 THEN 'fx'
            ELSE 'derivative'
        END AS asset_class,
        CAST(u_age * 30 AS INTEGER) AS trade_age_days
    FROM base
),
ops AS (
    SELECT
        *,
        -- reconciliation: unmatched clusters on risky counterparties
        CASE
            WHEN u_match < 0.012 + CASE WHEN cp_risky THEN 0.10 ELSE 0 END
                THEN 'unmatched'
            WHEN u_match < 0.020 + CASE WHEN cp_risky THEN 0.12 ELSE 0 END
                THEN 'partial'
            ELSE 'matched'
        END AS match_status,
        -- SSI validity degrades for risky counterparties and cross-border
        u_ssi >= (0.0018
                  + CASE WHEN cp_risky THEN 0.020 ELSE 0 END
                  + CASE WHEN cross_border THEN 0.0012 ELSE 0 END)
            AS ssi_valid,
        -- confirmations go missing more often when reconciliation broke
        u_conf >= (0.006
                   + CASE WHEN u_match < 0.03 THEN 0.10 ELSE 0 END
                   + CASE WHEN cp_risky THEN 0.015 ELSE 0 END)
            AS confirmation_received
    FROM entities
),
risk AS (
    SELECT
        *,
        -- Failures concentrate on recent trades (older exceptions get
        -- worked and resolved); a small model-detected pocket carries
        -- high predicted risk.
        LEAST(0.99,
            (0.002
             + CASE WHEN NOT ssi_valid THEN 0.55 ELSE 0 END
             + CASE WHEN match_status = 'unmatched' THEN 0.22 ELSE 0 END
             + CASE WHEN match_status = 'partial' THEN 0.10 ELSE 0 END
             + CASE WHEN NOT confirmation_received THEN 0.12 ELSE 0 END
             + CASE WHEN cp_risky THEN 0.03 ELSE 0 END
             + CASE WHEN cross_border THEN 0.002 ELSE 0 END
             + u_noise * 0.005
            ) * CASE WHEN trade_age_days < 5 THEN 1.0 ELSE 0.05 END
            -- model-detected pocket: high predicted risk at any age
            + CASE WHEN u_model < 0.004 THEN 0.85 ELSE 0 END
        ) AS fail_probability
    FROM ops
)
SELECT
    i + 1 AS transaction_id,
    'TRD-' || lpad(CAST(i + 1 AS VARCHAR), 10, '0') AS trade_id,
    'SRC-' || CAST(1 + (hash(i * 31 + {s + 40}) % 5) AS VARCHAR)
        AS source_system,
    'R-' || CAST(hash(i * 37 + {s + 41}) % 100000000 AS VARCHAR)
        AS source_record_id,

    as_of - INTERVAL 1 DAY * trade_age_days
        - INTERVAL 1 MINUTE * CAST(u_price * 480 AS INTEGER)
        AS trade_timestamp,
    CAST(as_of - INTERVAL 1 DAY * trade_age_days AS DATE) AS trade_date,
    CAST(as_of - INTERVAL 1 DAY * trade_age_days AS DATE)
        + CASE WHEN cross_border THEN 3 ELSE 2 END
        AS contractual_settle_date,
    CASE WHEN u_fail >= fail_probability
              AND trade_age_days >= CASE WHEN cross_border THEN 3 ELSE 2 END
         THEN CAST(as_of - INTERVAL 1 DAY
                   * (trade_age_days - CASE WHEN cross_border THEN 3 ELSE 2 END)
                   AS DATE)
    END AS actual_settle_date,
    CASE
        WHEN u_fail < fail_probability THEN 'exception'
        WHEN trade_age_days < CASE WHEN cross_border THEN 3 ELSE 2 END
            THEN 'open'
        ELSE 'complete'
    END AS lifecycle_status,
    as_of - INTERVAL 1 MINUTE * CAST(u_noise * 720 AS INTEGER)
        AS status_recorded_at,

    asset_class,
    'INS-' || CAST(1 + (hash(i * 41 + {s + 42}) % 5000) AS VARCHAR)
        AS instrument_id,
    CASE WHEN u_side < 0.5 THEN 'BUY' ELSE 'SELL' END AS side,
    CAST(1 + u_qty * 99999 AS BIGINT) AS quantity,
    ROUND(1 + u_price * 999, 4) AS price,
    ROUND((1 + u_qty * 99999) * (1 + u_price * 999), 2) AS gross_amount,
    CASE market
        WHEN 'US' THEN 'USD' WHEN 'EU' THEN 'EUR' WHEN 'UK' THEN 'GBP'
        WHEN 'JP' THEN 'JPY' ELSE 'HKD'
    END AS currency,
    -- heavy tail: ~1.5% of trades above roughly 30M USD
    ROUND(EXP(10 + u_notional * 8), 2) AS notional_usd,

    counterparty_id,
    1 + (hash(i * 43 + {s + 43}) % 40) AS legal_entity_id,
    1 + (hash(i * 47 + {s + 44}) % 8000) AS account_id,
    1 + (hash(i * 53 + {s + 45}) % 300) AS broker_id,
    1 + (hash(i * 59 + {s + 46}) % 25) AS custodian_id,

    match_status,
    CASE
        WHEN match_status = 'matched' THEN NULL
        WHEN u_break < 0.25 THEN 'economic_terms'
        WHEN u_break < 0.55 THEN 'quantity'
        WHEN u_break < 0.85 THEN 'ssi_mismatch'
        ELSE 'timing'
    END AS break_type,
    CASE WHEN match_status <> 'matched' AND u_break < 0.25
         THEN ROUND(u_noise * 5000, 2) END AS price_difference,
    CASE WHEN match_status <> 'matched' AND u_break >= 0.25
              AND u_break < 0.55
         THEN CAST(u_noise * 1000 AS BIGINT) END AS quantity_difference,
    CASE WHEN match_status <> 'matched'
         THEN ROUND(u_noise * 25000, 2) END AS cash_difference,
    CASE
        WHEN match_status = 'unmatched' THEN 1 + CAST(u_break * 4 AS INTEGER)
        WHEN match_status = 'partial' THEN 1 + CAST(u_break * 2 AS INTEGER)
        ELSE 0
    END AS fields_mismatched,

    CASE
        WHEN u_fail < fail_probability THEN 'failed'
        WHEN u_fail < fail_probability + 0.001 THEN 'cancelled'
        WHEN trade_age_days < CASE WHEN cross_border THEN 3 ELSE 2 END
            THEN 'pending'
        ELSE 'settled'
    END AS settlement_status,
    market,
    -- ~27% of trades have a cutoff within 2 hours; intersected with the
    -- unsettled population this approaches the plan's ~2% context
    as_of + INTERVAL 1 MINUTE * CAST(
        CASE WHEN u_cutoff < 0.27 THEN u_cutoff / 0.27 * 115
             ELSE 120 + u_cutoff * 2880 END AS INTEGER)
        AS market_cutoff_at,
    CASE WHEN cross_border THEN 'FOP' ELSE 'DVP' END AS settlement_method,
    'SSI-' || CAST(1 + (hash(i * 61 + {s + 47}) % 4000) AS VARCHAR) AS ssi_id,
    1 + (hash(i * 67 + {s + 48}) % 9) AS ssi_version,
    ssi_valid,
    CASE WHEN u_fail < fail_probability THEN
        CASE
            WHEN NOT ssi_valid THEN 'invalid_ssi'
            WHEN match_status <> 'matched' THEN 'unmatched'
            WHEN NOT confirmation_received THEN 'missing_confirmation'
            ELSE 'counterparty_short'
        END
    END AS fail_reason,

    confirmation_received,
    CASE WHEN confirmation_received
         THEN as_of - INTERVAL 1 DAY * trade_age_days
              + INTERVAL 1 MINUTE * CAST(u_docs * 1440 AS INTEGER)
    END AS confirmation_timestamp,
    ROUND(CASE WHEN confirmation_received
               THEN 0.90 + u_docs * 0.10
               ELSE 0.30 + u_docs * 0.45 END, 4)
        AS contract_match_confidence,
    CASE WHEN NOT confirmation_received
         THEN 1 + CAST(u_docs * 3 AS INTEGER) ELSE 0 END
        AS document_exception_count,

    CASE WHEN u_fail < fail_probability OR match_status <> 'matched'
         THEN as_of - INTERVAL 1 MINUTE * CAST(u_age * 2880 AS INTEGER)
    END AS exception_opened_at,
    CASE WHEN u_fail < fail_probability OR match_status <> 'matched'
         THEN CAST(u_age * 2880 AS INTEGER) ELSE 0 END
        AS exception_age_minutes,
    CASE
        WHEN NOT ssi_valid THEN 'settlements'
        WHEN match_status <> 'matched' THEN 'reconciliation'
        WHEN NOT confirmation_received THEN 'documentation'
        ELSE 'operations'
    END AS owner_team,
    -- manual touches grow with age and mismatch count
    CAST(u_age * 2880 / 480 AS INTEGER)
        + CASE
              WHEN match_status = 'unmatched'
                  THEN 2 + CAST(u_break * 4 AS INTEGER)
              WHEN match_status = 'partial'
                  THEN 1 + CAST(u_break * 2 AS INTEGER)
              ELSE 0
          END
        AS manual_touch_count,
    CASE WHEN cross_border THEN 240 ELSE 120 END AS sla_minutes,

    ROUND(LEAST(0.99, fail_probability * (0.85 + u_noise * 0.3)), 4)
        AS predicted_fail_probability,
    ROUND(CASE WHEN cp_risky THEN 0.60 + u_noise * 0.35
               ELSE u_cp * 0.40 END, 4)
        AS counterparty_risk_score,
    ROUND(u_anom * CASE WHEN u_fail < fail_probability THEN 1.0
                        ELSE 0.35 END, 4) AS anomaly_score,
    CASE
        WHEN NOT ssi_valid THEN 'repair_ssi'
        WHEN match_status = 'unmatched' THEN 'chase_counterparty'
        WHEN NOT confirmation_received THEN 'request_confirmation'
        WHEN u_fail < fail_probability THEN 'escalate'
        ELSE 'none'
    END AS recommended_action,

    as_of AS data_as_of,
    ROUND(0.80 + u_quality * 0.20, 4) AS source_quality_score,
    as_of - INTERVAL 1 MINUTE * CAST(u_quality * 1440 AS INTEGER)
        AS last_validated_at,
    CASE WHEN u_quality < 0.98 THEN 'clean' ELSE 'flagged' END
        AS audit_status
FROM risk;
"""


# Reference membership SQL for correctness checks (plan 9.3). Every bitmap
# count and every query result must reconcile against these.
REFERENCE_CONTEXT_SQL: dict = {
    "unmatched_trade": (
        "SELECT transaction_id FROM {table} WHERE match_status = 'unmatched'"
    ),
    "missing_confirmation": (
        "SELECT transaction_id FROM {table} "
        "WHERE confirmation_received = FALSE"
    ),
    "economic_terms_break": (
        "SELECT transaction_id FROM {table} "
        "WHERE break_type = 'economic_terms'"
    ),
    "invalid_ssi": (
        "SELECT transaction_id FROM {table} WHERE ssi_valid = FALSE"
    ),
    "settlement_overdue": (
        "SELECT transaction_id FROM {table} "
        "WHERE settlement_status NOT IN ('settled', 'cancelled') "
        "AND contractual_settle_date < CAST(TIMESTAMP '{as_of}' AS DATE)"
    ),
    "approaching_market_cutoff": (
        "SELECT transaction_id FROM {table} "
        "WHERE settlement_status NOT IN ('settled', 'cancelled') "
        "AND market_cutoff_at <= TIMESTAMP '{as_of}' + INTERVAL '2 hours'"
    ),
    "high_notional": (
        "SELECT transaction_id FROM {table} WHERE notional_usd > 58000000"
    ),
    "predicted_settlement_fail": (
        "SELECT transaction_id FROM {table} "
        "WHERE predicted_fail_probability >= 0.80"
    ),
    "settlement_intervention_required": (
        "SELECT transaction_id FROM {table} "
        "WHERE settlement_status NOT IN ('settled', 'cancelled') "
        "AND (contractual_settle_date < CAST(TIMESTAMP '{as_of}' AS DATE) "
        "     OR market_cutoff_at <= TIMESTAMP '{as_of}' + INTERVAL '2 hours') "
        "AND (match_status <> 'matched' OR ssi_valid = FALSE "
        "     OR confirmation_received = FALSE OR fields_mismatched > 0 "
        "     OR predicted_fail_probability >= 0.80)"
    ),
}


def reference_context_sql(
    name: str, *, table_name: str = "transactions", as_of: str = DEFAULT_AS_OF
) -> str:
    """Reference membership SQL for *name* bound to a table and as-of time."""
    template = REFERENCE_CONTEXT_SQL[name]
    return template.format(table=table_name, as_of=as_of)
