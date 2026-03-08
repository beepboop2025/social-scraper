"""AI-powered change analysis using Opus 4.6 via CLIProxyAPI."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx

from monitoring.health.source_health_checker import HealthCheckResult, HealthStatus, IST

CLIPROXY_URL = "http://localhost:8317/v1/chat/completions"
MODEL = "claude-opus-4-6"


def _format_results_for_llm(results: list[HealthCheckResult]) -> str:
    """Format health check results into a readable string for the LLM."""
    lines = []
    for r in results:
        date_str = r.last_data_date.isoformat() if r.last_data_date else "unknown"
        lines.append(
            f"- {r.source_name}: status={r.status.value}, "
            f"response_time={r.response_time_ms:.0f}ms, "
            f"structure_match={r.expected_structure_match}, "
            f"last_data_date={date_str}, "
            f"notes={r.notes}"
        )
    return "\n".join(lines)


def _format_validation_diffs(validation_results: list[tuple[str, bool, list[str]]]) -> str:
    """Format structure validation diffs for the LLM."""
    lines = []
    for name, match, diffs in validation_results:
        if not match and diffs:
            lines.append(f"- {name}: {'; '.join(diffs)}")
    return "\n".join(lines) if lines else "No structural changes detected."


async def analyze_changes(
    results: list[HealthCheckResult],
    validation_results: list[tuple[str, bool, list[str]]] | None = None,
) -> str:
    """
    Send health check results to Opus 4.6 for intelligent analysis.
    Returns a formatted report with actionable recommendations.
    """
    problems = [r for r in results if r.status != HealthStatus.HEALTHY]

    if not problems and not validation_results:
        healthy_count = len(results)
        return (
            f"# EconScraper Health Report — {datetime.now(IST).strftime('%Y-%m-%d %H:%M IST')}\n\n"
            f"🟢 ALL CLEAR: {healthy_count}/{healthy_count} sources healthy.\n"
            f"No structural changes detected. No action needed."
        )

    # Build the prompt
    all_results_text = _format_results_for_llm(results)
    problems_text = _format_results_for_llm(problems) if problems else "None"
    validation_text = (
        _format_validation_diffs(validation_results)
        if validation_results
        else "Not checked."
    )

    prompt = f"""You are monitoring an economic data scraper ("EconScraper") that collects data from Indian and global financial sources. Here are today's health check results:

=== ALL SOURCES ===
{all_results_text}

=== PROBLEMS (non-healthy) ===
{problems_text}

=== STRUCTURAL CHANGES (compared to baseline) ===
{validation_text}

For each issue:
1. What is likely happening (site redesign? API change? temporary outage?)
2. How urgent is this (data gap forming? can wait?)
3. What specific fix is needed in the scraper code
4. If a source is down, suggest alternative data sources

Also check:
- Are multiple sources showing similar issues? (could indicate network problem on our side)
- Is any source showing gradual degradation over recent checks?
- Any new Indian financial data APIs or sources worth adding?

Keep the report concise and actionable. Use this format:
🔴 BROKEN: [source] — [problem] — [fix needed]
🟡 WARNING: [source] — [what changed] — [action needed]
🟢 ALL CLEAR: [count] sources healthy

End with: IMMEDIATE ACTIONS, WATCH LIST, and NEW OPPORTUNITIES sections."""

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as client:
            response = await client.post(
                CLIPROXY_URL,
                json={
                    "model": MODEL,
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "You are an expert on Indian financial data infrastructure. "
                                "You understand RBI, SEBI, NSE, CCIL systems deeply. "
                                "You give precise, actionable monitoring reports."
                            ),
                        },
                        {"role": "user", "content": prompt},
                    ],
                },
            )
            response.raise_for_status()
            data = response.json()
            report = data["choices"][0]["message"]["content"]

    except httpx.ConnectError:
        report = (
            f"# EconScraper Health Report — {datetime.now(IST).strftime('%Y-%m-%d %H:%M IST')}\n\n"
            f"⚠️  CLIProxyAPI not reachable at {CLIPROXY_URL}\n"
            f"AI analysis skipped. Raw results below:\n\n"
            f"## Problems\n{problems_text}\n\n"
            f"## Structural Changes\n{validation_text}"
        )
    except Exception as e:
        report = (
            f"# EconScraper Health Report — {datetime.now(IST).strftime('%Y-%m-%d %H:%M IST')}\n\n"
            f"⚠️  AI analysis failed: {e}\n\n"
            f"## Problems\n{problems_text}\n\n"
            f"## Structural Changes\n{validation_text}"
        )

    # Prepend header
    header = f"# EconScraper Health Report — {datetime.now(IST).strftime('%Y-%m-%d %H:%M IST')}\n\n"
    if not report.startswith("# "):
        report = header + report

    return report
