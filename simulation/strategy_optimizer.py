"""Multi-Strategy Fibonacci Optimizer.

Tests 72 combinations of entry/stop/target/filter parameters on cached
gapper data and prints a ranked comparison table sorted by Net P&L.

Usage:
    python main.py --optimize
"""

import logging
import time as _time
from dataclasses import dataclass
from datetime import time
from itertools import product
from typing import Optional

import pandas as pd

from config.settings import (
    STARTING_CAPITAL,
    POSITION_SIZE_PCT,
    SLIPPAGE_PCT,
    COMMISSION_PER_SHARE,
    MIN_COMMISSION,
    FIB_MAX_ENTRIES_PER_DAY,
    FIB_WARMUP_BARS,
    FIB_ENTRY_WINDOW_END,
    DATA_DIR,
)
from simulation.fib_reversal_backtest import (
    FibReversalEngine,
    _is_hammer,
    _is_bullish_engulfing,
    _detect_reversal_candle,
    _resample_to_5min,
    _get_all_fib_prices,
    _get_fib_price_info,
)
from strategies.fibonacci_engine import find_anchor_candle, build_dual_series

logger = logging.getLogger("trading_bot.optimizer")

_h, _m = FIB_ENTRY_WINDOW_END.split(":")
ENTRY_WINDOW_END = time(int(_h), int(_m))


# ── Strategy Configuration ─────────────────────────────────

@dataclass
class StrategyConfig:
    name: str
    entry_type: str        # "reversal_candle" | "fib_bounce" | "fib_bounce_green"
    stop_type: str         # "trail_2pct" | "trail_3pct" | "trail_5pct" | "fib_stop"
    target_type: str       # "none" | "next_fib" | "fib_2up"
    half_range_filter: bool


STOP_PCT_MAP = {
    "trail_2pct": 0.02,
    "trail_3pct": 0.03,
    "trail_5pct": 0.05,
}


def _build_all_configs() -> list[StrategyConfig]:
    """Build all 72 strategy combinations."""
    entry_types = ["reversal_candle", "fib_bounce", "fib_bounce_green"]
    stop_types = ["trail_2pct", "trail_3pct", "trail_5pct", "fib_stop"]
    target_types = ["none", "next_fib", "fib_2up"]
    filter_opts = [True, False]

    configs = []
    for entry, stop, target, filt in product(entry_types, stop_types, target_types, filter_opts):
        short_entry = entry.replace("reversal_candle", "rev").replace("fib_bounce_green", "bncgrn").replace("fib_bounce", "bnc")
        short_stop = stop.replace("trail_", "t")
        short_target = target.replace("next_fib", "nfib").replace("fib_2up", "f2up")
        short_filter = "filt" if filt else "nofilt"
        name = f"{short_entry}_{short_stop}_{short_target}_{short_filter}"
        configs.append(StrategyConfig(
            name=name,
            entry_type=entry,
            stop_type=stop,
            target_type=target,
            half_range_filter=filt,
        ))
    return configs


# ── Entry / Stop / Target Logic ────────────────────────────

def _check_entry(
    config: StrategyConfig,
    df_5m: pd.DataFrame,
    i: int,
    fib_prices: list[float],
    half_range: float,
) -> tuple[bool, float, int, str]:
    """Check if bar i qualifies as an entry signal.

    Returns (signal, fib_level, fib_idx, pattern_name).
    """
    bar = df_5m.iloc[i]
    bar_low = float(bar["low"])
    bar_close = float(bar["close"])
    bar_open = float(bar["open"])

    # Half-range filter
    if config.half_range_filter and bar_close < half_range:
        return False, 0.0, -1, ""

    # Entry type check
    if config.entry_type == "reversal_candle":
        is_rev, pattern = _detect_reversal_candle(df_5m, i)
        if not is_rev:
            return False, 0.0, -1, ""
    elif config.entry_type == "fib_bounce":
        pattern = "fib_bounce"
    elif config.entry_type == "fib_bounce_green":
        if bar_close <= bar_open:
            return False, 0.0, -1, ""
        pattern = "fib_bounce_green"
    else:
        return False, 0.0, -1, ""

    # Find nearest fib to bar_low
    best_fib_idx = -1
    best_dist = float("inf")
    for idx, fp in enumerate(fib_prices):
        dist = abs(bar_low - fp)
        if dist < best_dist:
            best_dist = dist
            best_fib_idx = idx

    if best_fib_idx < 0:
        return False, 0.0, -1, ""

    fib_level = fib_prices[best_fib_idx]
    proximity = fib_level * 0.008 if fib_level > 0 else 0.01

    if best_dist > proximity:
        return False, 0.0, -1, ""

    # Fib must be support (at or below close)
    if fib_level > bar_close:
        return False, 0.0, -1, ""

    # For bounce entries: bar close must be above fib
    if config.entry_type in ("fib_bounce", "fib_bounce_green"):
        if bar_close <= fib_level:
            return False, 0.0, -1, ""

    return True, fib_level, best_fib_idx, pattern


def _get_initial_stop(
    config: StrategyConfig,
    fill_price: float,
    fib_prices: list[float],
    fib_idx: int,
) -> float:
    """Calculate initial stop price based on config."""
    if config.stop_type in STOP_PCT_MAP:
        return round(fill_price * (1 - STOP_PCT_MAP[config.stop_type]), 4)

    # fib_stop: next fib level below entry fib
    if config.stop_type == "fib_stop":
        if fib_idx > 0:
            return round(fib_prices[fib_idx - 1], 4)
        return round(fill_price * 0.97, 4)

    return round(fill_price * 0.97, 4)


def _get_target_price(
    config: StrategyConfig,
    fib_prices: list[float],
    fib_idx: int,
) -> Optional[float]:
    """Calculate target price based on config. Returns None for no-target."""
    if config.target_type == "none":
        return None

    if config.target_type == "next_fib":
        if fib_idx + 1 < len(fib_prices):
            return round(fib_prices[fib_idx + 1], 4)
        return None

    if config.target_type == "fib_2up":
        if fib_idx + 2 < len(fib_prices):
            return round(fib_prices[fib_idx + 2], 4)
        return None

    return None


# ── Parametric Trade Simulation ────────────────────────────

def _simulate_window_parametric(
    config: StrategyConfig,
    symbol: str,
    gap: dict,
    window_df: pd.DataFrame,
    fib_prices: list[float],
    capital: float,
) -> list[dict]:
    """Walk 5-min bars on gap day with parametric entry/stop/target.

    Returns list of trade dicts (independent of engine state).
    """
    prev_close = gap["prev_close"]
    warmup = FIB_WARMUP_BARS

    df_5m = _resample_to_5min(window_df)
    if len(df_5m) < warmup + 3:
        return []

    day_open = float(df_5m["open"].iloc[0])
    gap_high = float(df_5m["high"].iloc[:warmup].max())

    if gap_high <= prev_close * 1.05:
        return []

    if len(fib_prices) < 3:
        return []

    trades = []
    cash = capital
    entries_today = 0

    # Position state (in-window only)
    in_position = False
    pos_qty = 0
    pos_entry_price = 0.0
    pos_entry_time = None
    pos_entry_commission = 0.0
    pos_fib_level = 0.0
    pos_fib_idx = -1
    pos_pattern = ""
    pos_stop = 0.0
    pos_target = None  # None means no target
    highest_since_entry = 0.0

    # Signal state
    signal_pending = False
    signal_fib_level = 0.0
    signal_fib_idx = -1
    signal_pattern = ""

    running_high = day_open

    is_trailing = config.stop_type in STOP_PCT_MAP
    trail_pct = STOP_PCT_MAP.get(config.stop_type, 0.03)

    for i in range(warmup, len(df_5m)):
        bar = df_5m.iloc[i]
        bar_time = df_5m.index[i]
        bar_open = float(bar["open"])
        bar_high = float(bar["high"])
        bar_low = float(bar["low"])
        bar_close = float(bar["close"])

        bar_t = bar_time.time() if hasattr(bar_time, "time") else None

        running_high = max(running_high, bar_high)
        half_range = (day_open + running_high) / 2

        # Force close at entry window end
        if bar_t and bar_t >= ENTRY_WINDOW_END:
            if in_position:
                exit_comm = max(pos_qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
                pnl_gross = (bar_close - pos_entry_price) * pos_qty
                pnl_net = pnl_gross - pos_entry_commission - exit_comm
                cash += pos_qty * bar_close - exit_comm
                trades.append(_make_trade(
                    symbol, pos_qty, pos_entry_price, pos_entry_time,
                    bar_close, bar_time, "eod_close", pnl_gross, pnl_net,
                    pos_entry_commission + exit_comm, pos_fib_level, pos_pattern,
                    gap["gap_pct"],
                ))
                in_position = False
            break

        # ── Check exits ──
        if in_position:
            highest_since_entry = max(highest_since_entry, bar_high)

            # Check target hit first
            if pos_target is not None and bar_high >= pos_target:
                fill = min(bar_open, pos_target)
                fill = max(fill, pos_target)  # fill at target price
                exit_comm = max(pos_qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
                pnl_gross = (fill - pos_entry_price) * pos_qty
                pnl_net = pnl_gross - pos_entry_commission - exit_comm
                cash += pos_qty * fill - exit_comm
                trades.append(_make_trade(
                    symbol, pos_qty, pos_entry_price, pos_entry_time,
                    fill, bar_time, "target_hit", pnl_gross, pnl_net,
                    pos_entry_commission + exit_comm, pos_fib_level, pos_pattern,
                    gap["gap_pct"],
                ))
                in_position = False
                continue

            # Update trailing stop
            if is_trailing:
                new_trail = highest_since_entry * (1 - trail_pct)
                pos_stop = max(pos_stop, new_trail)

            # Check stop hit
            if bar_low <= pos_stop:
                fill = min(bar_open, pos_stop)
                fill = max(fill, 0.01)
                exit_comm = max(pos_qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
                pnl_gross = (fill - pos_entry_price) * pos_qty
                pnl_net = pnl_gross - pos_entry_commission - exit_comm
                cash += pos_qty * fill - exit_comm
                trades.append(_make_trade(
                    symbol, pos_qty, pos_entry_price, pos_entry_time,
                    fill, bar_time, "stop_hit", pnl_gross, pnl_net,
                    pos_entry_commission + exit_comm, pos_fib_level, pos_pattern,
                    gap["gap_pct"],
                ))
                in_position = False
            continue

        # ── Fill pending signal ──
        if signal_pending:
            signal_pending = False
            if entries_today >= FIB_MAX_ENTRIES_PER_DAY:
                continue

            fill_price = round(bar_open * (1 + SLIPPAGE_PCT), 4)
            qty = int((cash * POSITION_SIZE_PCT) / fill_price)
            if qty <= 0:
                continue

            commission = max(qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
            cost = qty * fill_price + commission
            if cost > cash:
                qty = int((cash - MIN_COMMISSION) / fill_price)
                if qty <= 0:
                    continue
                commission = max(qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
                cost = qty * fill_price + commission

            cash -= cost

            stop = _get_initial_stop(config, fill_price, fib_prices, signal_fib_idx)
            target = _get_target_price(config, fib_prices, signal_fib_idx)

            in_position = True
            pos_qty = qty
            pos_entry_price = fill_price
            pos_entry_time = bar_time
            pos_entry_commission = commission
            pos_fib_level = signal_fib_level
            pos_fib_idx = signal_fib_idx
            pos_pattern = signal_pattern
            pos_stop = stop
            pos_target = target
            highest_since_entry = bar_high
            entries_today += 1

            # Check immediate stop on entry bar
            if bar_low <= pos_stop:
                fill = min(bar_open, pos_stop)
                fill = max(fill, 0.01)
                exit_comm = max(pos_qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
                pnl_gross = (fill - pos_entry_price) * pos_qty
                pnl_net = pnl_gross - pos_entry_commission - exit_comm
                cash += pos_qty * fill - exit_comm
                trades.append(_make_trade(
                    symbol, pos_qty, pos_entry_price, pos_entry_time,
                    fill, bar_time, "stop_hit", pnl_gross, pnl_net,
                    pos_entry_commission + exit_comm, pos_fib_level, pos_pattern,
                    gap["gap_pct"],
                ))
                in_position = False

            # Check immediate target on entry bar
            elif pos_target is not None and bar_high >= pos_target:
                fill = pos_target
                exit_comm = max(pos_qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
                pnl_gross = (fill - pos_entry_price) * pos_qty
                pnl_net = pnl_gross - pos_entry_commission - exit_comm
                cash += pos_qty * fill - exit_comm
                trades.append(_make_trade(
                    symbol, pos_qty, pos_entry_price, pos_entry_time,
                    fill, bar_time, "target_hit", pnl_gross, pnl_net,
                    pos_entry_commission + exit_comm, pos_fib_level, pos_pattern,
                    gap["gap_pct"],
                ))
                in_position = False
            continue

        # ── Scan for entry ──
        if entries_today >= FIB_MAX_ENTRIES_PER_DAY:
            continue

        signal, fib_lvl, fib_idx, pattern = _check_entry(
            config, df_5m, i, fib_prices, half_range,
        )
        if signal:
            signal_pending = True
            signal_fib_level = fib_lvl
            signal_fib_idx = fib_idx
            signal_pattern = pattern

    # EOD cleanup: force close any open position
    if in_position:
        last = df_5m.iloc[-1]
        exit_price = float(last["close"])
        exit_time = df_5m.index[-1]
        exit_comm = max(pos_qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
        pnl_gross = (exit_price - pos_entry_price) * pos_qty
        pnl_net = pnl_gross - pos_entry_commission - exit_comm
        cash += pos_qty * exit_price - exit_comm
        trades.append(_make_trade(
            symbol, pos_qty, pos_entry_price, pos_entry_time,
            exit_price, exit_time, "eod_close", pnl_gross, pnl_net,
            pos_entry_commission + exit_comm, pos_fib_level, pos_pattern,
            gap["gap_pct"],
        ))

    return trades


def _make_trade(
    symbol: str, qty: int, entry_price: float, entry_time,
    exit_price: float, exit_time, exit_reason: str,
    pnl_gross: float, pnl_net: float, total_commission: float,
    fib_level: float, pattern: str, gap_pct: float,
) -> dict:
    pnl_pct = (exit_price - entry_price) / entry_price if entry_price > 0 else 0
    return {
        "symbol": symbol,
        "quantity": qty,
        "entry_fill": round(entry_price, 4),
        "entry_time": str(entry_time),
        "exit_fill": round(exit_price, 4),
        "exit_time": str(exit_time),
        "exit_reason": exit_reason,
        "pnl_gross": round(pnl_gross, 2),
        "pnl_net": round(pnl_net, 2),
        "pnl_pct": round(pnl_pct * 100, 2),
        "commission": round(total_commission, 2),
        "fib_level": round(fib_level, 4),
        "pattern": pattern,
        "gap_pct": round(gap_pct, 1),
    }


# ── Data Loading (reuse engine) ────────────────────────────

def _load_symbol_data(engine: FibReversalEngine, symbol: str) -> Optional[dict]:
    """Load and cache all data needed for one symbol.

    Returns dict with daily_5y, df_intra, dual, fib_prices, anchor_info,
    bars_by_date, or None on failure.
    """
    daily_5y = engine._download_daily_5y(symbol)
    if daily_5y is None:
        return None

    anchor = find_anchor_candle(daily_5y)
    if anchor is None:
        return None

    anchor_low, anchor_high, anchor_date = anchor
    dual = build_dual_series(anchor_low, anchor_high)

    df_intra = engine._download_intraday(symbol)
    if df_intra is None:
        return None
    if not hasattr(df_intra.index, "date"):
        return None

    bars_by_date: dict = {}
    for idx in df_intra.index:
        d = idx.date()
        bars_by_date.setdefault(d, []).append(idx)

    return {
        "dual": dual,
        "df_intra": df_intra,
        "bars_by_date": bars_by_date,
        "anchor_info": {
            "anchor_date": anchor_date,
            "anchor_low": anchor_low,
            "anchor_high": anchor_high,
        },
    }


def _get_fib_prices_for_gap(dual, gap_high: float, prev_close: float) -> list[float]:
    """Build filtered fib_prices list for a specific gap event."""
    fib_prices = _get_all_fib_prices(dual, gap_high)
    range_low = prev_close * 0.3
    range_high = gap_high * 2.5
    return [p for p in fib_prices if range_low <= p <= range_high]


# ── Results Aggregation ────────────────────────────────────

@dataclass
class StrategyResult:
    config: StrategyConfig
    trades: list
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    net_pnl: float = 0.0
    gross_pnl: float = 0.0
    total_commission: float = 0.0
    max_drawdown: float = 0.0
    profit_factor: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    win_rate: float = 0.0


def _aggregate_results(config: StrategyConfig, all_trades: list[dict]) -> StrategyResult:
    """Compute summary stats from a list of trades."""
    result = StrategyResult(config=config, trades=all_trades)
    result.total_trades = len(all_trades)
    if not all_trades:
        return result

    wins = [t for t in all_trades if t["pnl_net"] >= 0]
    losses = [t for t in all_trades if t["pnl_net"] < 0]

    result.winning_trades = len(wins)
    result.losing_trades = len(losses)
    result.net_pnl = sum(t["pnl_net"] for t in all_trades)
    result.gross_pnl = sum(t["pnl_gross"] for t in all_trades)
    result.total_commission = sum(t["commission"] for t in all_trades)
    result.win_rate = result.winning_trades / result.total_trades * 100

    total_win_pnl = sum(t["pnl_net"] for t in wins) if wins else 0
    total_loss_pnl = abs(sum(t["pnl_net"] for t in losses)) if losses else 0

    result.avg_win = total_win_pnl / len(wins) if wins else 0
    result.avg_loss = -total_loss_pnl / len(losses) if losses else 0
    result.profit_factor = total_win_pnl / total_loss_pnl if total_loss_pnl > 0 else float("inf") if total_win_pnl > 0 else 0

    # Max drawdown (sequential walk through trades)
    equity = STARTING_CAPITAL
    peak = equity
    max_dd = 0.0
    for t in sorted(all_trades, key=lambda x: x["entry_time"]):
        equity += t["pnl_net"]
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
    result.max_drawdown = max_dd

    return result


# ── Main Entry Point ───────────────────────────────────────

def run_optimizer() -> None:
    """Run all 72 strategy combinations and print ranked results."""
    t_start = _time.time()

    print("=" * 70)
    print("Multi-Strategy Fibonacci Optimizer")
    print(f"  Capital: ${STARTING_CAPITAL:,.0f} | 72 strategy combinations")
    print("=" * 70)

    # Phase 1: Load data via FibReversalEngine (reuse gapper + data loading)
    engine = FibReversalEngine(capital=STARTING_CAPITAL)
    engine._symbols = engine._scan_for_candidates()
    engine._find_gappers()

    if not engine._gappers:
        print("No gap events found. Cannot optimize.")
        return

    gap_by_symbol: dict[str, list[dict]] = {}
    for g in engine._gappers:
        gap_by_symbol.setdefault(g["symbol"], []).append(g)

    print(f"\n  Symbols: {len(engine._symbols)}")
    print(f"  Gap events: {len(engine._gappers)}")

    # Phase 2: Pre-load all symbol data (ONCE)
    print("\nLoading symbol data...")
    symbol_data: dict[str, dict] = {}
    for symbol in engine._symbols:
        if symbol not in gap_by_symbol:
            continue
        data = _load_symbol_data(engine, symbol)
        if data:
            symbol_data[symbol] = data

    print(f"  Loaded data for {len(symbol_data)} symbols")

    # Phase 3: Pre-compute fib_prices and 5min resampled data per gap event
    # to avoid recomputing inside each of the 72 strategy runs.
    gap_cache: dict[str, dict] = {}  # key: "SYMBOL_DATE"
    total_windows = 0
    for symbol, gaps in gap_by_symbol.items():
        if symbol not in symbol_data:
            continue
        sd = symbol_data[symbol]
        for gap in gaps:
            gap_date = gap["date_obj"]
            gd = gap_date.date() if hasattr(gap_date, "date") else pd.Timestamp(gap_date).date()
            if gd not in sd["bars_by_date"]:
                continue
            day_indices = sd["bars_by_date"][gd]
            day_df = sd["df_intra"].loc[day_indices]
            if len(day_df) < FIB_WARMUP_BARS + 5:
                continue

            df_5m = _resample_to_5min(day_df)
            if len(df_5m) < FIB_WARMUP_BARS + 3:
                continue

            gap_high = float(df_5m["high"].iloc[:FIB_WARMUP_BARS].max())
            if gap_high <= gap["prev_close"] * 1.05:
                continue

            fib_prices = _get_fib_prices_for_gap(sd["dual"], gap_high, gap["prev_close"])
            if len(fib_prices) < 3:
                continue

            cache_key = f"{symbol}_{gap['date']}"
            gap_cache[cache_key] = {
                "symbol": symbol,
                "gap": gap,
                "window_df": day_df,
                "fib_prices": fib_prices,
            }
            total_windows += 1

    print(f"  Valid trade windows: {total_windows}")

    # Phase 4: Run all 72 strategies
    configs = _build_all_configs()
    print(f"\nRunning {len(configs)} strategy combinations...")

    results: list[StrategyResult] = []
    for ci, config in enumerate(configs, 1):
        all_trades = []
        for cache_key, cached in gap_cache.items():
            window_trades = _simulate_window_parametric(
                config,
                cached["symbol"],
                cached["gap"],
                cached["window_df"],
                cached["fib_prices"],
                STARTING_CAPITAL,
            )
            all_trades.extend(window_trades)

        result = _aggregate_results(config, all_trades)
        results.append(result)

        if ci % 12 == 0 or ci == len(configs):
            print(f"  [{ci}/{len(configs)}] {config.name}: "
                  f"{result.total_trades} trades, {result.win_rate:.1f}% WR, "
                  f"P&L ${result.net_pnl:+,.2f}")

    # Phase 5: Sort and print ranked table
    results.sort(key=lambda r: r.net_pnl, reverse=True)

    elapsed = _time.time() - t_start

    print(f"\n{'='*120}")
    print(f"OPTIMIZER RESULTS — {len(configs)} strategies, "
          f"{total_windows} trade windows, {elapsed:.1f}s elapsed")
    print(f"{'='*120}")
    print(f"{'Rank':>4} | {'Strategy':<35} | {'Trades':>6} | {'WR%':>6} | "
          f"{'Net P&L':>10} | {'Max DD':>8} | {'PF':>6} | {'Avg W':>8} | {'Avg L':>8}")
    print("-" * 120)

    for rank, r in enumerate(results, 1):
        pf_str = f"{r.profit_factor:.2f}" if r.profit_factor < 100 else "Inf"
        print(f"{rank:>4} | {r.config.name:<35} | {r.total_trades:>6} | "
              f"{r.win_rate:>5.1f}% | ${r.net_pnl:>+9,.2f} | "
              f"{r.max_drawdown:>7.1%} | {pf_str:>6} | "
              f"${r.avg_win:>7.2f} | ${r.avg_loss:>7.2f}")

    # Phase 6: Save to CSV
    csv_path = DATA_DIR / "optimizer_results.csv"
    rows = []
    for rank, r in enumerate(results, 1):
        rows.append({
            "rank": rank,
            "strategy": r.config.name,
            "entry_type": r.config.entry_type,
            "stop_type": r.config.stop_type,
            "target_type": r.config.target_type,
            "half_range_filter": r.config.half_range_filter,
            "total_trades": r.total_trades,
            "winning_trades": r.winning_trades,
            "losing_trades": r.losing_trades,
            "win_rate": round(r.win_rate, 2),
            "net_pnl": round(r.net_pnl, 2),
            "gross_pnl": round(r.gross_pnl, 2),
            "total_commission": round(r.total_commission, 2),
            "max_drawdown": round(r.max_drawdown, 4),
            "profit_factor": round(r.profit_factor, 4) if r.profit_factor < 1000 else 9999,
            "avg_win": round(r.avg_win, 2),
            "avg_loss": round(r.avg_loss, 2),
        })
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    print(f"\nResults saved to {csv_path}")

    # Top 5 summary
    print(f"\n{'='*70}")
    print("TOP 5 STRATEGIES")
    print(f"{'='*70}")
    for i, r in enumerate(results[:5], 1):
        c = r.config
        print(f"\n  #{i}: {c.name}")
        print(f"      Entry: {c.entry_type} | Stop: {c.stop_type} | "
              f"Target: {c.target_type} | Filter: {'ON' if c.half_range_filter else 'OFF'}")
        print(f"      Trades: {r.total_trades} | WR: {r.win_rate:.1f}% | "
              f"Net P&L: ${r.net_pnl:+,.2f} | Max DD: {r.max_drawdown:.1%}")
