# Benchmark Reports

Machine-specific measurements produced by `benchmarks/post_trade_benchmark.py`
(plan section 12). Environment details are embedded in each report; numbers
are not universal guarantees.

## post-trade-10m-2026-07-24

10,000,000 deterministic transactions, Roaring snapshots, Windows x86_64
(8 logical CPUs), Python 3.11.15, DuckDB 1.5.5, PyRoaring 1.1.0. The run used
clean source commit `439f03605d1955beb91f79bac4810b7eba99d764`.

| Measurement | Result |
|---|---|
| Dataset generation | 86.29 s, 855.1 MB on disk |
| Cold context materialization | 0.935 s average (0.654-1.670 s, 9 contexts) |
| Bitmap vs reference SQL counts | 9/9 exact matches |
| Union (3 contexts, 275,127 members) | 0.923 ms |
| Intersection (2 contexts) | 18.865 ms |
| Difference | 0.447 ms |
| Warm top-20 intervention query | 753 ms, snapshot semi-join pushdown |
| Connector + native REMOTE narrowing | 46,703 IDs, exact match, `roaring64` |
| Peak RSS | 1,270.4 MB |

Acceptance gates (plan section 12): all correctness gates pass. The selective
top-20 query uses snapshot pushdown; its 20 returned rows exactly match the SQL
reference. These are machine-specific observations, not performance
guarantees.

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
