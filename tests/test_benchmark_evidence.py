"""Credibility checks for the checked-in 10M benchmark evidence."""
from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).parents[1]
REPORT_PATH = (
    ROOT / "docs" / "benchmarks" / "post-trade-10m-2026-07-24.json"
)


def _report() -> dict:
    return json.loads(REPORT_PATH.read_text(encoding="utf-8"))


def test_benchmark_identifies_a_clean_source_commit():
    report = _report()
    environment = report["environment"]
    assert report["rows"] == 10_000_000
    assert environment["git_dirty"] is False
    assert len(environment["git_sha"]) == 40
    assert all(
        context["correctness_ok"]
        for context in report["contexts"].values()
    )
    assert all(
        result["correctness_ok"]
        for result in report["algebra"].values()
    )
    assert report["top20_query"]["exact_sql_match"] is True
    assert report["connector_remote"]["exact_reference_match"] is True


def test_dynamic_html_embeds_the_exact_benchmark_artifact():
    html = (
        ROOT / "demo" / "post_trade_demo_visual.html"
    ).read_text(encoding="utf-8")
    start = '<script id="report-data" type="application/json">'
    payload = html.split(start, 1)[1].split("</script>", 1)[0]
    embedded = json.loads(payload)
    assert embedded["benchmark"] == _report()


def test_benchmark_readme_uses_artifact_measurements():
    report = _report()
    readme = (
        ROOT / "docs" / "benchmarks" / "README.md"
    ).read_text(encoding="utf-8")
    assert f"{report['generation']['seconds']:.2f} s" in readme
    assert (
        f"{report['top20_query']['warm_seconds'] * 1000:.0f} ms"
        in readme
    )
    assert report["environment"]["git_sha"] in readme
