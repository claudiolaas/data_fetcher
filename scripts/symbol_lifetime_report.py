"""Build an interactive symbol lifetime report without extending the main CLI."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from data_fetcher.providers.crypto import DEFAULT_EXCHANGE, CryptoDataFetcher


def _read_symbol_file(path: Path) -> List[str]:
    symbols: List[str] = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                symbols.append(line)
    return symbols


def _symbol_rows(
    fetcher: CryptoDataFetcher,
    symbols: Optional[str],
    symbols_file: Optional[Path],
    quote: Optional[str],
    active_only: bool,
    spot_only: bool,
    base: Optional[str],
    contains: Optional[str],
    limit: int,
) -> List[Dict]:
    explicit_symbols: List[str] = []
    if symbols:
        explicit_symbols.extend(s.strip() for s in symbols.split(",") if s.strip())
    if symbols_file:
        explicit_symbols.extend(_read_symbol_file(symbols_file))

    if explicit_symbols:
        markets = fetcher.get_markets()
        rows = []
        for symbol in explicit_symbols:
            info = markets.get(symbol, {})
            rows.append(
                {
                    "symbol": symbol,
                    "base": info.get("base", ""),
                    "quote": info.get("quote", ""),
                    "type": info.get("type", ""),
                    "active": info.get("active", ""),
                }
            )
        return rows

    rows = fetcher.get_symbols(
        quote=quote,
        active_only=active_only,
        spot_only=spot_only,
    )
    if base:
        rows = [r for r in rows if r.get("base") == base]
    if contains:
        rows = [r for r in rows if contains.upper() in r["symbol"].upper()]
    if limit > 0:
        rows = rows[:limit]
    return rows


def _write_csv(path: Path, rows: Iterable[Dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "exchange",
        "symbol",
        "base",
        "quote",
        "type",
        "active",
        "timeframe",
        "start_ms",
        "start_date_utc",
        "visible_until_utc",
        "method",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_lifetime_csv(path: Path) -> List[Dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _write_html(path: Path, rows: List[Dict], title: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(rows)
    escaped_title = escape(title)
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escaped_title}</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #f7f7f5;
      color: #1d2329;
    }}
    header {{
      padding: 22px 28px 12px;
      border-bottom: 1px solid #d9d9d4;
      background: #ffffff;
    }}
    h1 {{
      margin: 0 0 6px;
      font-size: 22px;
      font-weight: 650;
      letter-spacing: 0;
    }}
    .meta {{
      color: #59636e;
      font-size: 13px;
    }}
    main {{
      padding: 20px 28px 32px;
      display: grid;
      gap: 18px;
    }}
    .panel {{
      background: #ffffff;
      border: 1px solid #d9d9d4;
      border-radius: 6px;
      padding: 12px;
    }}
    .controls {{
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
    }}
    input {{
      min-width: 260px;
      padding: 8px 10px;
      border: 1px solid #bfc4ca;
      border-radius: 4px;
      font-size: 14px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    th, td {{
      padding: 7px 8px;
      border-bottom: 1px solid #e4e5e3;
      text-align: left;
      white-space: nowrap;
    }}
    th {{
      background: #f1f2ef;
      font-weight: 650;
    }}
  </style>
</head>
<body>
  <header>
    <h1>{escaped_title}</h1>
    <div class="meta" id="summary"></div>
  </header>
  <main>
    <section class="panel controls">
      <label for="symbolSearch">Symbol search</label>
      <input id="symbolSearch" type="search" placeholder="BTC/USDT">
      <span class="meta" id="matchSummary"></span>
    </section>
    <section class="panel">
      <h2 style="font-size: 16px; margin: 0 0 10px;">Oldest Symbols</h2>
      <div id="oldestTable"></div>
    </section>
    <section class="panel"><div id="timeline"></div></section>
    <section class="panel"><div id="cumulative"></div></section>
    <section class="panel"><div id="heatmap"></div></section>
  </main>
  <script>
    const rows = {payload}.filter(r => r.start_date_utc && r.start_date_utc !== "N/A");
    rows.sort((a, b) => new Date(a.start_date_utc) - new Date(b.start_date_utc));

    const activeRows = rows.filter(r => String(r.active).toLowerCase() === "true");
    const inactiveRows = rows.filter(r => String(r.active).toLowerCase() !== "true");
    document.getElementById("summary").textContent =
      `${{rows.length}} symbols with start dates, ${{activeRows.length}} active, ` +
      `${{inactiveRows.length}} inactive or unknown`;

    function renderOldestTable(sourceRows) {{
      const rowsForTable = sourceRows.slice(0, 40);
      const html = [
        "<table>",
        "<thead><tr><th>Symbol</th><th>Start</th><th>Active</th><th>Type</th><th>Method</th></tr></thead>",
        "<tbody>",
        ...rowsForTable.map(r =>
          `<tr><td>${{r.symbol}}</td><td>${{r.start_date_utc}}</td><td>${{r.active}}</td>` +
          `<td>${{r.type || ""}}</td><td>${{r.method}}</td></tr>`
        ),
        "</tbody></table>"
      ].join("");
      document.getElementById("oldestTable").innerHTML = html;
    }}

    function matchingRows() {{
      const query = document.getElementById("symbolSearch").value.trim().toUpperCase();
      if (!query) return rows;
      return rows.filter(r => r.symbol.toUpperCase().includes(query));
    }}

    renderOldestTable(rows);

    function lineTrace(sourceRows, name, color) {{
      const x = [];
      const y = [];
      const text = [];
      for (const r of sourceRows) {{
        x.push(r.start_date_utc, r.visible_until_utc, null);
        y.push(r.symbol, r.symbol, null);
        text.push(
          `${{r.symbol}}<br>start: ${{r.start_date_utc}}<br>active: ${{r.active}}`,
          `${{r.symbol}}<br>visible until: ${{r.visible_until_utc}}`,
          null
        );
      }}
      return {{
        type: "scattergl",
        mode: "lines",
        x,
        y,
        text,
        hoverinfo: "text",
        line: {{ color, width: 5 }},
        name
      }};
    }}

    function renderTimeline(sourceRows) {{
      const active = sourceRows.filter(r => String(r.active).toLowerCase() === "true");
      const inactive = sourceRows.filter(r => String(r.active).toLowerCase() !== "true");
      document.getElementById("matchSummary").textContent =
        sourceRows.length === rows.length ? "" : `${{sourceRows.length}} matching symbols`;
      Plotly.react("timeline", [
        lineTrace(inactive, "inactive or unknown", "#b7bbc2"),
        lineTrace(active, "active", "#27895b")
      ], {{
        title: "Symbol Lifetimes",
        height: Math.max(420, Math.min(1800, sourceRows.length * 24 + 140)),
        margin: {{ l: 150, r: 28, t: 46, b: 46 }},
        xaxis: {{ title: "time" }},
        yaxis: {{ automargin: true, type: "category", categoryorder: "array", categoryarray: sourceRows.map(r => r.symbol) }},
        legend: {{ orientation: "h" }}
      }}, {{ responsive: true }});
    }}

    renderTimeline(rows);
    document.getElementById("symbolSearch").addEventListener("input", () => {{
      const filtered = matchingRows();
      renderOldestTable(filtered);
      renderTimeline(filtered);
    }});

    const monthly = new Map();
    for (const r of rows) {{
      const d = new Date(r.start_date_utc);
      const key = `${{d.getUTCFullYear()}}-${{String(d.getUTCMonth() + 1).padStart(2, "0")}}`;
      monthly.set(key, (monthly.get(key) || 0) + 1);
    }}
    const sortedMonths = Array.from(monthly.keys()).sort();
    let total = 0;
    const cumulative = sortedMonths.map(m => {{
      total += monthly.get(m);
      return total;
    }});

    Plotly.newPlot("cumulative", [{{
      type: "scatter",
      mode: "lines+markers",
      x: sortedMonths,
      y: cumulative,
      line: {{ color: "#2f6fbb", width: 2 }},
      marker: {{ size: 5 }},
      name: "listed symbols"
    }}], {{
      title: "Cumulative Listed Symbols",
      height: 360,
      margin: {{ l: 58, r: 28, t: 46, b: 70 }},
      xaxis: {{ title: "first candle month" }},
      yaxis: {{ title: "count" }}
    }}, {{ responsive: true }});

    const years = Array.from(new Set(rows.map(r => new Date(r.start_date_utc).getUTCFullYear()))).sort();
    const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    const z = years.map(year => months.map((_, idx) => {{
      const key = `${{year}}-${{String(idx + 1).padStart(2, "0")}}`;
      return monthly.get(key) || 0;
    }}));

    Plotly.newPlot("heatmap", [{{
      type: "heatmap",
      x: months,
      y: years,
      z,
      colorscale: "YlGnBu",
      hovertemplate: "%{{y}} %{{x}}<br>new symbols: %{{z}}<extra></extra>"
    }}], {{
      title: "Monthly New Symbol Starts",
      height: Math.max(320, years.length * 32 + 120),
      margin: {{ l: 58, r: 28, t: 46, b: 46 }}
    }}, {{ responsive: true }});
  </script>
</body>
</html>
"""
    path.write_text(html)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a Binance/CCXT symbol lifetime CSV and HTML report."
    )
    parser.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    parser.add_argument("--quote")
    parser.add_argument("--base")
    parser.add_argument("--contains")
    parser.add_argument("--symbols")
    parser.add_argument("--symbols-file", type=Path)
    parser.add_argument("--timeframe", default="1h")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--active-only", action="store_true")
    parser.add_argument("--all-types", action="store_true")
    parser.add_argument("--from-csv", type=Path, help="Render HTML from an existing lifetime CSV without probing")
    parser.add_argument("--csv-output", type=Path, default=Path("symbol_lifetimes.csv"))
    parser.add_argument("--html-output", type=Path, default=Path("symbol_lifetimes.html"))
    args = parser.parse_args()

    if args.from_csv:
        rows = _read_lifetime_csv(args.from_csv)
        title = f"{args.exchange} {args.quote or ''} symbol lifetimes".strip()
        _write_html(args.html_output, rows, title=title)
        print(f"Wrote HTML to {args.html_output}")
        return

    fetcher = CryptoDataFetcher(exchange_id=args.exchange)
    symbol_rows = _symbol_rows(
        fetcher=fetcher,
        symbols=args.symbols,
        symbols_file=args.symbols_file,
        quote=args.quote,
        active_only=args.active_only,
        spot_only=not args.all_types,
        base=args.base,
        contains=args.contains,
        limit=args.limit,
    )

    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rows = []
    for index, row in enumerate(symbol_rows, start=1):
        symbol = row["symbol"]
        print(f"[{index}/{len(symbol_rows)}] probing {symbol}")
        start_ms, method = fetcher.fetch_earliest_timestamp(symbol, args.timeframe)
        start_iso = fetcher.exchange.iso8601(start_ms) if start_ms is not None else "N/A"
        rows.append(
            {
                "exchange": args.exchange,
                "symbol": symbol,
                "base": row.get("base", ""),
                "quote": row.get("quote", ""),
                "type": row.get("type", ""),
                "active": row.get("active", ""),
                "timeframe": args.timeframe,
                "start_ms": start_ms if start_ms is not None else "",
                "start_date_utc": start_iso,
                "visible_until_utc": now_iso,
                "method": method,
            }
        )

    title = f"{args.exchange} {args.quote or ''} symbol lifetimes".strip()
    _write_csv(args.csv_output, rows)
    _write_html(args.html_output, rows, title=title)
    print(f"Wrote CSV to {args.csv_output}")
    print(f"Wrote HTML to {args.html_output}")


if __name__ == "__main__":
    main()
