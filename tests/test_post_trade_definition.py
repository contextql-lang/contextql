"""Parser proof for the planned post-trade context definition.

Plan gate (docs/plans/post-trade-roaring-contexts.md section 6.4): before
implementation, parser tests must prove that every expression in the planned
``settlement_intervention_required`` definition is supported by the current
grammar.
"""

SETTLEMENT_INTERVENTION_REQUIRED = """
CREATE CONTEXT settlement_intervention_required
ON transaction_id
SCORE intervention_priority
TEMPORAL (status_recorded_at, MINUTE)
DESCRIPTION 'Transactions requiring action to prevent or resolve settlement failure'
TAGS ('post-trade', 'settlement', 'operations')
CLASSIFICATION internal
WITH (
    materialized = TRUE,
    storage = 'roaring',
    refresh_mode = 'incremental',
    refresh_interval = '1 minute',
    stale_after = '2 minutes',
    history = TRUE,
    history_retention = '90 days',
    source_watermark = status_recorded_at
)
AS
SELECT
    transaction_id,
    status_recorded_at,
    LEAST(
        1.0,
        predicted_fail_probability
        + CASE WHEN ssi_valid = FALSE THEN 0.20 ELSE 0 END
        + CASE WHEN match_status <> 'matched' THEN 0.15 ELSE 0 END
    ) AS intervention_priority
FROM transactions
WHERE settlement_status NOT IN ('settled', 'cancelled')
  AND (
      contractual_settle_date < CURRENT_DATE
      OR market_cutoff_at <= CURRENT_TIMESTAMP + INTERVAL '2 hours'
  )
  AND (
      match_status <> 'matched'
      OR ssi_valid = FALSE
      OR confirmation_received = FALSE
      OR fields_mismatched > 0
      OR predicted_fail_probability >= 0.80
  );
"""

REGISTER_MCP_DEEPSEE = """
REGISTER MCP PROVIDER deepsee.settlement_risk
ENDPOINT 'resolved-by-deployment'
TRANSPORT HTTPS
ENTITY_TYPE transaction
ENTITY_KEY_TYPE INT64
TIMEOUT 5000
ON_FAILURE warn
DESCRIPTION 'Settlement-risk membership provider';
"""

REGISTER_REMOTE_DEEPSEE = """
REGISTER REMOTE PROVIDER deepsee
ENDPOINT 'resolved-by-deployment'
TRANSPORT HTTPS
ENTITY_TYPE transaction
ENTITY_KEY_TYPE INT64
RESOURCES (settlement_cases, reconciliation_evidence)
TIMEOUT 10000
ON_FAILURE error
DESCRIPTION 'Operational evidence provider';
"""

DEMO_QUERY = """
SELECT
    t.transaction_id,
    t.counterparty_id,
    t.asset_class,
    t.notional_usd,
    t.contractual_settle_date,
    d.break_type,
    d.recommended_action,
    CONTEXT_SCORE() AS intervention_priority
FROM transactions AS t
LEFT JOIN REMOTE(deepsee.settlement_cases) AS d
  ON t.transaction_id = d.transaction_id
WHERE CONTEXT ON t IN (
    settlement_intervention_required,
    MCP(deepsee.settlement_risk) WEIGHT 1.5
)
ORDER BY CONTEXT DESC
LIMIT 20;
"""


class TestKeywordPrefixedIdentifiers:
    """Keyword terminals must not match inside identifiers (word-boundary
    regression: 'notional_usd' once tokenized as NOT + 'ional_usd')."""

    def test_identifiers_with_keyword_prefixes_parse(self, parser):
        parser.parse(
            "SELECT notional_usd, selection, fromage, inner_flag "
            "FROM orders WHERE notional_usd > 58000000 AND andes = 1;"
        )

    def test_definition_text_reconstruction_intact(self, parser):
        from contextql.semantic import SemanticLowerer
        model = SemanticLowerer().lower(
            parser.parse(
                "CREATE CONTEXT high_notional ON transaction_id AS "
                "SELECT transaction_id FROM transactions "
                "WHERE notional_usd > 58000000;"
            )
        )[0]
        assert "notional_usd" in model.definition_sql


class TestPlannedDefinitionParses:
    """The complete planned statements must parse with the current grammar."""

    def test_settlement_intervention_required_parses(self, parser):
        tree = parser.parse(SETTLEMENT_INTERVENTION_REQUIRED)
        assert tree.data == "start"

    def test_mcp_provider_registration_parses(self, parser):
        tree = parser.parse(REGISTER_MCP_DEEPSEE)
        assert tree.data == "start"

    def test_remote_provider_registration_parses(self, parser):
        tree = parser.parse(REGISTER_REMOTE_DEEPSEE)
        assert tree.data == "start"

    def test_demo_query_parses(self, parser):
        tree = parser.parse(DEMO_QUERY)
        assert tree.data == "start"
