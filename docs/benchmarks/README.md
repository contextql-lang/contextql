# Benchmark Reports

Machine-specific measurements produced by `benchmarks/post_trade_benchmark.py`
(plan section 12). Environment details are embedded in each report; numbers
are not universal guarantees.

## post-trade-10m-2026-07-23

10,000,000 deterministic transactions, Roaring snapshots, Linux x86_64
(24 cores), Python 3.12, DuckDB 1.5.5, pyroaring.

| Measurement | Result |
|---|---|
| Dataset generation | 26.8 s, 854 MB on disk |
| Cold context materialization | ~0.26 s per context (9 contexts) |
| Bitmap vs reference SQL counts | 9/9 exact matches |
| Union (3 contexts, 275,127 members) | 0.7 ms |
| Intersection (2 contexts) | 0.27 ms |
| Difference | 0.21 ms |
| Warm top-20 intervention query | 293 ms, snapshot semi-join pushdown |
| Peak RSS | 1.45 GB |

Acceptance gates (plan section 12): all pass. Warm bitmap algebra is ~370x
faster than rebuilding membership; selective queries do not transfer ten
million rows into Pandas.

Context cardinalities at 10M (deterministic, reported not forced):

| Context | Members | Serialized bytes |
|---|---:|---:|
| unmatched_trade | 162,384 | 326,025 |
| missing_confirmation | 96,145 | 193,547 |
| economic_terms_break | 62,411 | 126,079 |
| invalid_ssi | 32,832 | 66,921 |
| settlement_overdue | 43,316 | 87,889 |
| approaching_market_cutoff | 197,266 | 395,789 |
| high_notional | 154,914 | 311,085 |
| predicted_settlement_fail | 28,244 | 57,745 |
| settlement_intervention_required | 36,712 | 74,681 |

The `settlement_intervention_required` cardinality (36,712 ≈ 0.37%) exceeds
the plan section 9.3 estimate of 4,000-15,000 but remains highly selective;
per the plan, exact counts are deterministic and reported rather than forced.
