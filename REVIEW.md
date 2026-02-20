# Screen Monitor — Code Review & Improvement Plan

## Date: 2026-02-20
## File: `monitor/screen_monitor.py` (4,722 lines) + `monitor/order_thread.py` (353 lines)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        App (Tkinter GUI)                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐ │
│  │ Stock    │  │ Portfolio│  │ Trading  │  │ Alerts      │ │
│  │ Table    │  │ Panel    │  │ Panel    │  │ Panel (5)   │ │
│  │ (scroll) │  │ (P&L)   │  │ BUY/SELL │  │             │ │
│  └──────────┘  └──────────┘  └──────────┘  └─────────────┘ │
│  ┌──────────┐  ┌──────────────────┐  ┌──────────────────┐  │
│  │ Scanner  │  │ Settings         │  │ Filter/Start     │  │
│  │ Slots(3) │  │ Freq/Alert/Price │  │ 20%+ | START     │  │
│  └──────────┘  └──────────────────┘  └──────────────────┘  │
└─────────────────┬──────────────┬────────────────────────────┘
                  │              │
    ┌─────────────▼──┐    ┌─────▼──────────┐
    │ ScannerThread  │    │ OrderThread    │
    │ (clientId=20)  │    │ (clientId=21)  │
    │                │    │                │
    │ • OCR scan     │    │ • Execute buy  │
    │ • IBKR scan    │    │ • Execute sell │
    │ • Enrichment   │    │ • FIB DT order │
    │ • Alerts       │    │ • Account data │
    │ • FIB DT auto  │    │                │
    │ • Telegram bot │    └────────────────┘
    └────────────────┘
         │
    ┌────▼────────────┐
    │ TelegramListener│
    │ (long-poll)     │
    │ /stock, @bot    │
    └─────────────────┘
```

### Threading Model
- **Main Thread**: Tkinter GUI event loop
- **ScannerThread**: Main scan cycle (OCR → IBKR fallback → enrich → alerts → FIB DT)
- **OrderThread**: Independent IBKR connection for order execution + account polling
- **TelegramListenerThread**: Long-polls Telegram getUpdates for stock lookups

### Data Flow
1. OCR/IBKR scan → `current` dict (symbol → price/pct/volume/etc.)
2. Enrichment (Finviz + IBKR news + Fib levels) for stocks ≥16%
3. Alerts (HOD/LOD/Fib/VWAP/Spike) for enriched stocks ≥20%
4. FIB DT auto-strategy picks best turnover stock → entry signals
5. GUI updated via `root.after(0, callback)` for thread safety

---

## 2. Current Features

### Data Sources
| Source | What | When |
|--------|------|------|
| OCR (Webull/any window) | Symbol, price, %chg, volume, float | Primary scan |
| IBKR reqScannerData | TOP_PERC_GAIN + HOT_BY_VOLUME + MOST_ACTIVE | Fallback scan |
| IBKR reqHistoricalData | RVOL, VWAP, prev_close, day_high, day_low | Enrichment ≥16% |
| Finviz (finvizfinance) | Float, short%, EPS, news, company info | Enrichment ≥16% |
| IBKR reqHistoricalNews | DJ/FLY/BRFG headlines | Enrichment + periodic check |

### Alerts System (Telegram Group Chat)
| Alert | Threshold | Condition |
|-------|-----------|-----------|
| Full stock report | ≥16% + VWAP above + float<70M + news | New discovery |
| HOD Break | ≥20% | Price breaks day high |
| FIB Touch x2 | ≥20% | 2nd touch of fib level (0.8% proximity) |
| LOD Touch x2 | ≥20% | 2nd touch of day low (0.5% proximity) |
| VWAP Cross | enriched | Price crosses VWAP (10-min cooldown) |
| Spike | ≥20% | 8%+ rise in 1-3 min window (5-min cooldown) |

### Telegram Messages (Private Chat)
- Startup notification
- Session transitions (pre-market → market → after-hours)
- Market open reminders (30/15/5 min)
- Holiday alerts
- Stocks In Play summary (9:30 ET)
- Session summaries (end of pre/market/AH)
- Daily full report (16:05 ET)

### Trading
- Manual: BUY 25/50/75/100% / SELL 25/50/75/100% via GUI
- Auto: FIB DT strategy (best turnover stock, split-exit bracket)
- FIB DT manual button in GUI
- All orders routed through OrderThread (clientId 21)

---

## 3. What Works Well

1. **Modular scan pipeline** — OCR → IBKR fallback is elegant and resilient
2. **Enrichment cache with TTL** — avoids redundant Finviz/IBKR calls
3. **Split-exit FIB DT orders** — OCA bracket + standalone stop is proper IBKR usage
4. **In-place GUI updates** — fast path avoids full table rebuild (no flicker)
5. **Thread-safe GUI** — all `root.after(0, ...)` callbacks
6. **Daily state reset** — clean slate each trading day
7. **Warmup mode** — suppresses alerts on first scan cycle
8. **Session awareness** — different behavior for pre-market/market/AH
9. **News translation** — batch Hebrew translation is efficient
10. **Telegram message splitting** — handles >4096 char messages

---

## 4. Issues & Improvement Opportunities

### A. Code Organization (Priority: Medium)
**Problem**: `screen_monitor.py` is 4,722 lines in a single file. Everything is mixed: GUI, scanning, alerts, Telegram, charting, FIB DT strategy, order logic, news, helpers.

**Suggestion**: Split into modules:
```
monitor/
  __init__.py
  app.py              # App class (GUI only) — ~1,200 lines
  scanner_thread.py   # ScannerThread class — ~700 lines
  alerts.py           # All alert functions (HOD/LOD/FIB/VWAP/Spike) — ~300 lines
  enrichment.py       # _enrich_stock, fetch_stock_info, news — ~500 lines
  telegram.py         # All Telegram send functions + listener — ~400 lines
  charts.py           # generate_fib_chart, chart window — ~400 lines
  fibonacci.py        # calc_fib_levels, _format_fib_text — ~200 lines
  ibkr_scan.py        # _run_ibkr_scan, _run_ocr_scan — ~400 lines
  summaries.py        # Daily/session/stocks-in-play summaries — ~500 lines
  order_thread.py     # (already separate)
```
**Impact**: Easier to debug, test, and modify individual features without risking others.

---

### B. Performance Issues (Priority: High)

#### B1. Sequential IBKR enrichment is slow
**Problem**: In `_cycle()`, enrichment loops through stocks one-by-one, each making multiple IBKR API calls (daily bars, intraday bars across 6 timeframes). For 10+ stocks, this takes 2-5 minutes.

**Current flow** (per stock):
```
_enrich_stock → Finviz API (~1s)
             → IBKR news (~1s)
             → calc_fib_levels → IBKR 5Y daily bars (~3s)
_build_stock_report → 6× IBKR intraday bars (~6s) for MA table
```
Total per stock: ~11s. With 10 stocks: ~2 minutes of enrichment.

**Suggestion**:
- Use `ThreadPoolExecutor` to enrich 3 stocks in parallel (IBKR rate limit allows ~3 concurrent)
- Skip MA table download during scan cycles (only needed for Telegram reports)
- Cache daily data longer (it rarely changes)

#### B2. Quick OCR refresh runs every second
**Problem**: `_quick_ocr_price_update()` runs every 1 second between cycles. Each OCR call takes ~1s itself (screenshot + resize + OCR). This means the scanner thread is almost always doing OCR.

**Suggestion**: Reduce OCR refresh to every 3-5 seconds, or only when market is open.

#### B3. MA table downloads 6 timeframes for every stock report
**Problem**: `_build_stock_report()` downloads 1m/5m/15m/1h/4h/W bars from IBKR. This is ~6 API calls per report, and is done even for stocks that may not be interesting.

**Suggestion**: Only compute MA table for lookup requests (Telegram /stock), not for auto-discovery reports.

---

### C. Reliability Issues (Priority: High)

#### C1. No error recovery in enrichment loop
**Problem**: If Finviz throws a rate-limit error or IBKR disconnects mid-enrichment, the exception is logged but that stock is never retried.

**Suggestion**: Add retry queue for failed enrichments, process on next cycle.

#### C2. Contract cache never expires
**Problem**: `_contract_cache` is only cleared on day reset. If a symbol is delisted or changes exchange mid-day, the stale contract could cause order failures.

**Suggestion**: Add TTL to contract cache (e.g., 4 hours).

#### C3. IBKR connection shared between scan and enrichment
**Problem**: `_get_ibkr()` returns a single IB instance used by both scanning and enrichment. If a long-running enrichment blocks, the scan cycle is delayed.

**Suggestion**: Consider a separate IBKR connection for enrichment (clientId 22), or at least queue enrichment for after the scan completes.

#### C4. Telegram listener has no error backoff
**Problem**: `TelegramListenerThread._poll()` has a basic `time_mod.sleep(3)` on error, but no exponential backoff. If Telegram API is down, it hammers the API every 3 seconds.

**Suggestion**: Add exponential backoff (5s → 10s → 20s → 60s max).

---

### D. Feature Gaps (Priority: Medium)

#### D1. No historical alert review
**Problem**: Alerts are sent to Telegram and logged to CSV, but there's no way to review past alerts in the GUI.

**Suggestion**: Add scrollable alert history panel or separate "Alert Log" tab.

#### D2. No stock watchlist
**Problem**: Can't pin favorite stocks to always track. If a stock drops below 16%, it disappears from enrichment.

**Suggestion**: Add watchlist feature — manually added symbols are always enriched and monitored.

#### D3. No sound alerts
**Problem**: All alerts are visual (Telegram + GUI panel). During active trading, it's easy to miss critical alerts like HOD breaks.

**Suggestion**: Add optional sound alerts (system bell or audio file) for high-priority alerts.

#### D4. No pre-market gap scanner
**Problem**: The scanner waits for OCR/IBKR data which may be sparse in pre-market. No dedicated pre-market gap-up scanner.

**Suggestion**: At 7:00 ET, run a dedicated IBKR scan for pre-market gainers (separate scan code).

#### D5. Charts window has no auto-refresh
**Problem**: Chart popup shows static images. If price changes significantly, charts are stale.

**Suggestion**: Add refresh button or auto-refresh every 60s in chart window.

#### D6. No position P&L chart/graph
**Problem**: Portfolio panel shows current P&L but no historical P&L tracking.

**Suggestion**: Track P&L over time in SQLite, show mini P&L chart.

---

### E. UI/UX Issues (Priority: Low-Medium)

#### E1. Column widths are fixed
**Problem**: `width=8` for all columns. Symbols like "AAPL" and prices like "$0.12" waste space.

**Suggestion**: Use proportional layout or auto-resize columns.

#### E2. No keyboard shortcuts
**Problem**: Everything requires mouse clicks. No keyboard navigation for quick trading.

**Suggestion**: Add keyboard shortcuts:
- `↑/↓` to navigate stocks
- `Enter` to select
- `B` for buy, `S` for sell
- `C` for chart
- `F` to toggle filter

#### E3. Trading panel has no stop-loss input
**Problem**: BUY orders go out as plain limit orders without stop-loss. User must manually set stops.

**Suggestion**: Add optional stop-loss % input next to price field.

#### E4. Alert panel only shows 5 alerts
**Problem**: Only last 5 alerts visible. Important alerts scroll off quickly.

**Suggestion**: Make alerts scrollable, or add "critical alerts" that stick.

#### E5. No dark/light theme toggle
Not critical but nice to have for different lighting conditions.

---

### F. Data Quality Issues (Priority: Medium)

#### F1. OCR is inherently unreliable
**Problem**: OCR depends on window position, font rendering, screen resolution. Errors like "I" vs "1", "O" vs "0" happen.

**Suggestion**: Add OCR confidence scoring. If a symbol can't be found in IBKR contract database, flag it as suspicious.

#### F2. VWAP calculation in pre-market is approximate
**Problem**: Extended-hours VWAP is computed as simple average of (H+L+C)/3, not proper volume-weighted.

**Suggestion**: Use proper VWAP formula: sum(price × volume) / sum(volume) from minute bars.

#### F3. Float data from Finviz may be stale
**Problem**: Finviz updates float data irregularly. For recent offerings/dilutions, the float could be wrong.

**Suggestion**: Cross-reference with IBKR fundamental data when available.

---

### G. Security & Robustness (Priority: Medium)

#### G1. Bot token in environment variable
This is correct practice. No issues here.

#### G2. No input validation on manual symbol entry
**Problem**: `_on_sym_entry` accepts any string. Could crash if non-ASCII is entered.

**Suggestion**: Validate: 1-5 uppercase ASCII letters only.

#### G3. Order confirmation is a simple dialog
**Problem**: `messagebox.askokcancel` could be accidentally clicked. No "type to confirm" for large orders.

**Suggestion**: For orders > $1,000, require typing the symbol to confirm.

---

## 5. Quick Wins (Can do today)

| # | What | Impact | Effort |
|---|------|--------|--------|
| 1 | Add sound alert for HOD/Spike | High | 10 min |
| 2 | Chart window refresh button | Medium | 5 min |
| 3 | Keyboard shortcuts (↑↓ Enter) | Medium | 20 min |
| 4 | Telegram backoff in listener | Low | 5 min |
| 5 | Validate symbol entry input | Low | 3 min |
| 6 | Show enrichment progress bar | Medium | 15 min |
| 7 | Add stock watchlist (save to state) | High | 30 min |

---

## 6. Medium-Term Improvements

| # | What | Impact | Effort |
|---|------|--------|--------|
| 1 | Split into modules | Maintainability | 2-3 hrs |
| 2 | Parallel enrichment (ThreadPool) | Performance | 1 hr |
| 3 | Reduce OCR frequency to 3s | Performance | 5 min |
| 4 | Enrichment retry queue | Reliability | 30 min |
| 5 | P&L history tracking | Trading insight | 1 hr |
| 6 | Pre-market gap scanner at 7am | Feature | 30 min |
| 7 | IBKR connection pool (scan vs enrich) | Reliability | 1 hr |

---

## 7. Lines of Code Breakdown

| Section | Lines | % |
|---------|-------|---|
| IBKR Connection & Scan | ~450 | 10% |
| OCR Scanner | ~250 | 5% |
| Enrichment (Finviz+IBKR) | ~400 | 8% |
| Fibonacci (calc+chart) | ~350 | 7% |
| MA Table & Stock Report | ~200 | 4% |
| News (fetch+translate+periodic) | ~200 | 4% |
| Alerts (HOD/LOD/FIB/VWAP/Spike) | ~250 | 5% |
| Telegram (send/listen/lookup) | ~400 | 8% |
| Summaries (daily/session/SIP) | ~350 | 7% |
| FIB DT Strategy Integration | ~250 | 5% |
| GUI (App class) | ~1,050 | 22% |
| Trading Panel | ~250 | 5% |
| Chart Window | ~200 | 4% |
| File Logger & Helpers | ~150 | 3% |
| Market Session & Reminders | ~150 | 3% |
| **Total** | **~4,722** | **100%** |

---

## 8. Decision Log

*Record decisions made about improvements here:*

| Date | Decision | Reason |
|------|----------|--------|
| | | |
| | | |

---

*Generated by Claude Code — 2026-02-20*
