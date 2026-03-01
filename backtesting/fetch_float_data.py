"""Fetch float + avg volume data from Finviz for backtest symbols.

Saves to data/backtest_cache/float_data.json.

Usage:
    python backtesting/fetch_float_data.py
"""

import json
import sys
import time
from pathlib import Path

import pandas as pd

_PROJECT_ROOT = Path(__file__).parent.parent
_CACHE_DIR = _PROJECT_ROOT / "data" / "backtest_cache"
_OUTPUT = _CACHE_DIR / "float_data.json"


def _parse_float_str(s: str) -> float:
    """Parse Finviz float string like '2.14M' or '120.5K' to share count."""
    if not s or s == '-':
        return 0.0
    s = s.strip().upper()
    try:
        if s.endswith('B'):
            return float(s[:-1]) * 1_000_000_000
        if s.endswith('M'):
            return float(s[:-1]) * 1_000_000
        if s.endswith('K'):
            return float(s[:-1]) * 1_000
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def _parse_vol_str(s: str) -> float:
    """Parse Finviz avg volume string like '1.5M' to share count."""
    return _parse_float_str(s)  # same format


def fetch_all():
    from finvizfinance.quote import finvizfinance as Finviz

    # Load existing cache
    existing = {}
    if _OUTPUT.exists():
        try:
            existing = json.loads(_OUTPUT.read_text())
        except Exception:
            pass

    # Get all symbols from gappers CSVs
    symbols = set()
    for csv_name in ["gappers_bot_stocks.csv", "gappers_20pct.csv"]:
        csv_path = _CACHE_DIR / csv_name
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            symbols.update(df["symbol"].unique())

    symbols = sorted(symbols)
    # Skip already fetched
    to_fetch = [s for s in symbols if s not in existing]
    print(f"Total symbols: {len(symbols)}, already cached: {len(existing)}, to fetch: {len(to_fetch)}")

    for i, sym in enumerate(to_fetch):
        try:
            stock = Finviz(sym)
            fund = stock.ticker_fundament()
            float_str = fund.get('Shs Float', '-')
            avg_vol_str = fund.get('Avg Volume', '-')
            short_str = fund.get('Short Float', '-')

            float_shares = _parse_float_str(float_str)
            avg_vol = _parse_vol_str(avg_vol_str)

            existing[sym] = {
                "float_str": float_str,
                "float_shares": float_shares,
                "avg_vol_str": avg_vol_str,
                "avg_vol": avg_vol,
                "short_str": short_str,
            }
            print(f"  [{i+1}/{len(to_fetch)}] {sym:8s} float={float_str:>10s} ({float_shares:>14,.0f})  "
                  f"avg_vol={avg_vol_str:>10s} ({avg_vol:>12,.0f})")

        except Exception as e:
            err = str(e)
            if "404" in err or "not found" in err.lower():
                existing[sym] = {"float_str": "-", "float_shares": 0, "avg_vol_str": "-", "avg_vol": 0, "error": "404"}
                print(f"  [{i+1}/{len(to_fetch)}] {sym:8s} — not found on Finviz")
            else:
                print(f"  [{i+1}/{len(to_fetch)}] {sym:8s} — ERROR: {e}")

        # Save after each fetch (resume-friendly)
        _OUTPUT.write_text(json.dumps(existing, indent=2))

        # Rate limit: Finviz blocks fast requests
        time.sleep(1.0)

    print(f"\nDone. Saved {len(existing)} symbols to {_OUTPUT}")

    # Summary
    has_float = sum(1 for v in existing.values() if v.get("float_shares", 0) > 0)
    has_vol = sum(1 for v in existing.values() if v.get("avg_vol", 0) > 0)
    print(f"  With float data: {has_float}/{len(existing)}")
    print(f"  With avg volume: {has_vol}/{len(existing)}")


if __name__ == "__main__":
    fetch_all()
