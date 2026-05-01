# Polymarket 5m Crypto Ask-Trigger Bot

Buys $1 of any 5-minute crypto market on Polymarket whenever the ask
hits 0.58 (configurable). Watches BTC, ETH, XRP, DOGE, HYPE, BNB, SOL.
Holds to resolution.

Adapted from a weather-market bot's architecture — same safety triple,
same V2 SDK execution patterns, same neg_risk lookup and balance-delta
fill verification.

## Strategy

For every active `{coin}-updown-5m-{ts}` market:

- Poll the best ASK on both UP and DOWN tokens every 2s.
- When `ENTRY_THRESHOLD <= ask <= ENTRY_MAX_PRICE`, place a $1 BUY.
- Hold to resolution. Each share resolves $1 if your side wins.
- Skip if `seconds_left < MIN_SECONDS_REMAINING` (default 20s).
- One position per (slug, side) — no double-buying the same market.

## Safety triple

Three independent flags must all be true for live trading:

```
DRY_RUN=false  AND  TRADING_ENABLED=true  AND  ARMED_FOR_LIVE=true
```

Default is dry-run. Recommended order:
1. Run with `DRY_RUN=true` for a few hours, watch logs and SQLite.
2. Set `DRY_RUN=false` and `TRADING_ENABLED=true` (still paper because `ARMED_FOR_LIVE=false`).
3. When you're ready, flip `ARMED_FOR_LIVE=true`. That's the live pin.

## Local run

```bash
cp .env.example .env
# edit .env — keep the safety triple all-false at first
pip install -r requirements.txt
python bot.py
```

Visit `http://localhost:8080/healthz` for status JSON.

## Inspect the SQLite

```bash
sqlite3 bot.sqlite3

# Today's stats
SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN status='closed' AND pnl_usd>0 THEN 1 ELSE 0 END) AS wins,
  SUM(CASE WHEN status='closed' AND pnl_usd<0 THEN 1 ELSE 0 END) AS losses,
  ROUND(SUM(CASE WHEN status='closed' THEN pnl_usd ELSE 0 END), 2) AS pnl,
  SUM(CASE WHEN status='open' THEN 1 ELSE 0 END) AS open_n
FROM positions
WHERE substr(ts,1,10) = strftime('%Y-%m-%d', 'now');

# Last 20 trades
SELECT ts, coin, side, entry_price, shares, status, pnl_usd
FROM positions ORDER BY id DESC LIMIT 20;

# By coin, last 7 days
SELECT coin,
       COUNT(*) FILTER (WHERE status='closed') AS closed,
       SUM(CASE WHEN pnl_usd>0 THEN 1 ELSE 0 END) AS wins,
       ROUND(SUM(pnl_usd), 2) AS pnl
FROM positions
WHERE ts >= date('now', '-7 days')
GROUP BY coin;
```

## Deploy to Fly.io

```bash
# First time
fly launch --no-deploy --copy-config         # picks up fly.toml
fly volume create poly5m_data --size 1 --region ams

# Set secrets (NEVER put these in fly.toml or .env in the repo)
fly secrets set POLY_PRIVATE_KEY=0x...
fly secrets set POLY_FUNDER=0x...

# Deploy in dry-run first
fly deploy

# Watch it
fly logs

# Once happy, flip to live (one at a time)
fly secrets set DRY_RUN=false
fly secrets set TRADING_ENABLED=true
fly secrets set ARMED_FOR_LIVE=true
fly deploy

# Emergency stop
fly secrets set KILL_SWITCH=true
fly deploy

# Or unarm (keeps the bot running, just stops new entries)
fly secrets set ARMED_FOR_LIVE=false
fly deploy
```

## Git setup

```bash
git init
git add .
git commit -m "initial"
git remote add origin git@github.com:YOU/poly5m-bot.git
git push -u origin main
```

`.env` is in `.gitignore`. **Never** commit `POLY_PRIVATE_KEY`. Only ever
set keys via `fly secrets set` or local `.env`.

## Files

- `bot.py` — the whole bot (discovery, price polling, executor, persistence)
- `requirements.txt` — pinned deps (`py-clob-client-v2`, `httpx`, etc.)
- `Dockerfile` — Python 3.12 slim, deps + bot
- `fly.toml` — Fly config: volume mount, healthcheck, env defaults
- `.env.example` — fill this in as `.env` for local runs
- `.gitignore` — keeps `.env` and `*.sqlite3` out of git

## Tuning notes

- **`PRICE_POLL_INTERVAL_SECONDS`**: 2s is fine for most cases. Drop to 1s
  if you're seeing too many missed entries (the ask was at 0.58 between
  polls and you didn't see it).
- **`MIN_SECONDS_REMAINING`**: 20s is conservative. The fill round-trip is
  3-12s plus matching delay. Don't go below 15.
- **`MAX_CONCURRENT_POSITIONS`**: at 7 coins × 2 sides × ~5 windows of overlap
  during an active streak you can hit ~70 concurrent. Cap at 20-30 to bound risk.
- **`MAX_DAILY_LOSS_USD`**: kicks in *after* a position closes red. With
  14 markets/min potentially firing this can blow through fast. Watch the first
  day live closely.

## Known limits

- Polymarket is geo-restricted in many jurisdictions. Make sure your
  use is permitted where you are.
- These markets resolve on Chainlink BTC/ETH/etc. price feeds. Final
  resolution can lag the window close by 30-90s.
- The bot does not currently sell positions early — it always holds to
  resolution. If you want take-profit/stop-loss on the way to resolution,
  port the `_maybe_exit` logic from the weather bot's `strategy.py`.
