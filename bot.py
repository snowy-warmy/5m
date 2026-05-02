"""
Polymarket 5m crypto ask-trigger bot — websocket version with debug endpoints.

Strategy: subscribe to the CLOB market WS for every active 5m crypto market.
Maintain local order books. When the best ASK on UP or DOWN reaches
ENTRY_THRESHOLD, fire a $1 BUY (one per side per window).

Debug endpoints:
  /          → JSON status (markets, asks, triggers, today's P&L)
  /healthz   → same
  /debug     → HTML dashboard, auto-refreshes every 3s
  /ws-log    → last 200 raw websocket messages (for diagnosing WS issues)
  /trades    → last 50 orders + positions

Safety triple — all three required for live trading:
    DRY_RUN=false  AND  TRADING_ENABLED=true  AND  ARMED_FOR_LIVE=true
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
from collections import deque
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
import websockets

# ─── env ──────────────────────────────────────────────────────────────────
def _env(key: str, default: Any, cast=str):
    v = os.environ.get(key)
    if v is None or v == "":
        return default
    if cast is bool:
        return v.lower() in ("1", "true", "yes", "on")
    try:
        return cast(v)
    except Exception:
        return default


try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


COINS = [c.strip().lower() for c in _env("COINS", "btc").split(",") if c.strip()]

# ENTRY_THRESHOLD: comma-separated ladder, e.g. "0.40,0.60" → 2 entries per side.
# Single value like "0.58" still works (1 entry per side).
def _parse_thresholds(raw: str) -> list[float]:
    out = []
    for tok in str(raw).split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            v = float(tok)
            if 0 < v < 1:
                out.append(v)
        except ValueError:
            pass
    return sorted(set(out))

_THRESHOLD_RAW = os.environ.get("ENTRY_THRESHOLD", "0.65")
ENTRY_THRESHOLDS = _parse_thresholds(_THRESHOLD_RAW) or [0.65]
# Back-compat: keep ENTRY_THRESHOLD as the LOWEST rung for any code that
# might still reference it.
ENTRY_THRESHOLD = ENTRY_THRESHOLDS[0]

ENTRY_MAX_PRICE = _env("ENTRY_MAX_PRICE", 0.97, float)
POSITION_SIZE_USD = _env("POSITION_SIZE_USD", 1.0, float)

DISCOVERY_REFRESH_SECONDS = _env("DISCOVERY_REFRESH_SECONDS", 10, int)
MIN_SECONDS_REMAINING = _env("MIN_SECONDS_REMAINING", 20, int)

# Strategy: "threshold" (existing market-cross logic) or "limits" (rest GTC limits at open)
STRATEGY = _env("STRATEGY", "threshold").lower()

# For STRATEGY=limits: comma-separated limit prices. Each fires both UP and DOWN.
# e.g. LIMIT_PRICES=0.40,0.50 means 4 orders per market: UP@.40, UP@.50, DOWN@.40, DOWN@.50
def _parse_limit_prices(raw: str) -> list[float]:
    out = []
    for tok in str(raw).split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            v = float(tok)
            if 0 < v < 1:
                out.append(v)
        except ValueError:
            pass
    return sorted(set(out))

LIMIT_PRICES = _parse_limit_prices(os.environ.get("LIMIT_PRICES", "0.40,0.50")) or [0.40, 0.50]

# Pre-market buy ladder: place BUY limits at these prices on both UP and DOWN
# the moment the market is created. Default: [0.40, 0.45].
LIMIT_BUY_PRICES = _parse_limit_prices(os.environ.get("LIMIT_BUY_PRICES", "0.40,0.45")) or [0.40, 0.45]
# When a BUY fills, immediately place a SELL at this price on the same side.
LIMIT_SELL_PRICE = _env("LIMIT_SELL_PRICE", 0.50, float)
# Cancel unfilled BUY limits N seconds after window opens (give a tiny grace period).
CANCEL_BUYS_AT_OPEN_DELAY = _env("CANCEL_BUYS_AT_OPEN_DELAY", 0, int)

MAX_CONCURRENT_POSITIONS = _env("MAX_CONCURRENT_POSITIONS", 0, int)
MAX_DAILY_LOSS_USD = _env("MAX_DAILY_LOSS_USD", 50.0, float)
KILL_SWITCH = _env("KILL_SWITCH", False, bool)

DRY_RUN = _env("DRY_RUN", True, bool)
TRADING_ENABLED = _env("TRADING_ENABLED", False, bool)
ARMED_FOR_LIVE = _env("ARMED_FOR_LIVE", False, bool)

GAMMA_BASE_URL = _env("GAMMA_BASE_URL", "https://gamma-api.polymarket.com")
CLOB_BASE_URL = _env("CLOB_BASE_URL", "https://clob.polymarket.com")
CLOB_WS_URL = _env("CLOB_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
POLYMARKET_HOST = _env("POLYMARKET_HOST", "https://clob.polymarket.com")
POLYMARKET_CHAIN_ID = _env("POLYMARKET_CHAIN_ID", 137, int)
POLY_SIGNATURE_TYPE = _env("POLY_SIGNATURE_TYPE", 2, int)
POLY_PRIVATE_KEY = _env("POLY_PRIVATE_KEY", "")
POLY_FUNDER = _env("POLY_FUNDER", "")
POLY_AUTO_DERIVE_CREDS = _env("POLY_AUTO_DERIVE_CREDS", True, bool)
POLY_API_KEY = _env("POLY_API_KEY", "")
POLY_API_SECRET = _env("POLY_API_SECRET", "")
POLY_API_PASSPHRASE = _env("POLY_API_PASSPHRASE", "")

DB_PATH = _env("DB_PATH", "/data/bot.sqlite3")
LOG_LEVEL = _env("LOG_LEVEL", "INFO")
HEALTHCHECK_PORT = _env("HEALTHCHECK_PORT", 8080, int)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-7s %(name)-9s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bot")


def is_live_mode() -> bool:
    return TRADING_ENABLED and not DRY_RUN and ARMED_FOR_LIVE and not KILL_SWITCH


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─── debug ringbuffer ─────────────────────────────────────────────────────
class DebugLog:
    """Ring buffer for diagnostic events visible via /debug page."""
    def __init__(self, ws_max=200, evt_max=200) -> None:
        self.ws_messages: deque = deque(maxlen=ws_max)
        self.events: deque = deque(maxlen=evt_max)
        self.connect_count = 0
        self.message_count = 0
        self.last_ws_connect_at: Optional[float] = None
        self.last_ws_message_at: Optional[float] = None
        self.last_book_snapshot_at: Optional[float] = None
        self.last_price_change_at: Optional[float] = None
        self.unknown_event_types: dict[str, int] = {}

    def event(self, msg: str) -> None:
        self.events.append({"t": time.time(), "msg": msg})
        log.info(msg)

    def ws_msg(self, raw: str, kind: str = "in") -> None:
        self.ws_messages.append({"t": time.time(), "kind": kind, "raw": raw[:1000]})
        if kind == "in":
            self.message_count += 1
            self.last_ws_message_at = time.time()


debug = DebugLog()


# ─── models ───────────────────────────────────────────────────────────────
@dataclass
class CryptoMarket:
    coin: str
    window_ts: int
    slug: str
    event_id: str
    condition_id: str
    up_token_id: str
    down_token_id: str
    min_tick_size: str = "0.01"
    neg_risk: bool = False

    @property
    def end_ts(self) -> int:
        return self.window_ts + 300

    def seconds_left(self) -> int:
        return max(0, self.end_ts - int(time.time()))


@dataclass
class Position:
    side: str
    coin: str
    slug: str
    condition_id: str
    token_id: str
    entry_price: float = 0.0
    size_usd: float = 0.0
    shares: float = 0.0
    entry_time_iso: str = ""
    neg_risk: bool = False
    order_id: Optional[str] = None
    status: str = "open"


@dataclass
class TokenBook:
    asks: dict[float, float] = field(default_factory=dict)
    bids: dict[float, float] = field(default_factory=dict)
    last_update_ts: float = 0.0
    snapshot_count: int = 0
    delta_count: int = 0

    def best_ask(self) -> Optional[float]:
        return min(self.asks.keys()) if self.asks else None

    def best_bid(self) -> Optional[float]:
        return max(self.bids.keys()) if self.bids else None

    def replace_book(self, asks: list[tuple[float, float]],
                     bids: list[tuple[float, float]]) -> None:
        self.asks = {p: s for p, s in asks if s > 0}
        self.bids = {p: s for p, s in bids if s > 0}
        self.snapshot_count += 1
        self.last_update_ts = time.time()

    def apply_changes(self, side: str, changes: list[tuple[float, float]]) -> None:
        target = self.asks if side == "ask" else self.bids
        for price, size in changes:
            if size <= 0:
                target.pop(price, None)
            else:
                target[price] = size
        self.delta_count += 1
        self.last_update_ts = time.time()


# ─── persistence ──────────────────────────────────────────────────────────
class Persistence:
    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._init()

    @contextmanager
    def _conn(self):
        c = sqlite3.connect(self.path)
        c.row_factory = sqlite3.Row
        try:
            yield c
            c.commit()
        finally:
            c.close()

    def _init(self) -> None:
        with self._conn() as c:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT, side TEXT, coin TEXT, slug TEXT,
                condition_id TEXT, token_id TEXT,
                entry_price REAL, size_usd REAL, shares REAL,
                neg_risk INTEGER, order_id TEXT,
                status TEXT, pnl_usd REAL, exit_reason TEXT
            );
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT, action TEXT, coin TEXT, slug TEXT,
                condition_id TEXT, token_id TEXT, side TEXT,
                price REAL, size_usd REAL, shares REAL,
                status TEXT, order_id TEXT, payload TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_pos_status ON positions(status);
            CREATE INDEX IF NOT EXISTS idx_pos_slug ON positions(slug);
            """)

    def record_order(self, **k) -> int:
        with self._conn() as c:
            cur = c.execute(
                """INSERT INTO orders
                (ts,action,coin,slug,condition_id,token_id,side,price,size_usd,shares,status,order_id,payload)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (k["ts"], k["action"], k["coin"], k["slug"], k["condition_id"],
                 k["token_id"], k["side"], k["price"], k["size_usd"], k["shares"],
                 k["status"], k.get("order_id"), json.dumps(k.get("payload") or {})),
            )
            return cur.lastrowid

    def record_position_open(self, p: Position) -> int:
        with self._conn() as c:
            cur = c.execute(
                """INSERT INTO positions
                (ts,side,coin,slug,condition_id,token_id,entry_price,size_usd,shares,neg_risk,order_id,status)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (p.entry_time_iso, p.side, p.coin, p.slug, p.condition_id, p.token_id,
                 p.entry_price, p.size_usd, p.shares, int(p.neg_risk), p.order_id, p.status),
            )
            return cur.lastrowid

    def update_position_resolved(self, pos_id: int, pnl: float, reason: str) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE positions SET status='closed', pnl_usd=?, exit_reason=? WHERE id=?",
                (pnl, reason, pos_id),
            )

    def open_count(self) -> int:
        with self._conn() as c:
            return c.execute("SELECT COUNT(*) AS n FROM positions WHERE status='open'").fetchone()["n"]

    def open_positions(self) -> list[dict]:
        with self._conn() as c:
            return [dict(r) for r in c.execute("SELECT * FROM positions WHERE status='open'").fetchall()]

    def has_open_for_slug_side(self, slug: str, side: str) -> bool:
        with self._conn() as c:
            r = c.execute(
                "SELECT 1 FROM positions WHERE slug=? AND side=? AND status='open' LIMIT 1",
                (slug, side),
            ).fetchone()
            return r is not None

    def daily_pnl(self, day: str) -> float:
        with self._conn() as c:
            r = c.execute(
                "SELECT COALESCE(SUM(pnl_usd),0) AS p FROM positions "
                "WHERE status='closed' AND substr(ts,1,10)=?",
                (day,),
            ).fetchone()
            return float(r["p"] or 0.0)

    def stats_today(self) -> dict:
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._conn() as c:
            r = c.execute(
                """SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN status='closed' AND pnl_usd>0 THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN status='closed' AND pnl_usd<0 THEN 1 ELSE 0 END) AS losses,
                    COALESCE(SUM(CASE WHEN status='closed' THEN pnl_usd ELSE 0 END),0) AS pnl,
                    SUM(CASE WHEN status='open' THEN 1 ELSE 0 END) AS open_n
                FROM positions WHERE substr(ts,1,10)=?""",
                (day,),
            ).fetchone()
            return dict(r) if r else {}

    def recent_orders(self, n: int = 50) -> list[dict]:
        with self._conn() as c:
            return [dict(r) for r in c.execute(
                "SELECT * FROM orders ORDER BY id DESC LIMIT ?", (n,)
            ).fetchall()]

    def recent_positions(self, n: int = 50) -> list[dict]:
        with self._conn() as c:
            return [dict(r) for r in c.execute(
                "SELECT * FROM positions ORDER BY id DESC LIMIT ?", (n,)
            ).fetchall()]


# ─── REST client ──────────────────────────────────────────────────────────
class PolyClient:
    def __init__(self) -> None:
        self.http = httpx.AsyncClient(
            timeout=10.0,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            headers={
                # Use a real browser UA. Cloudflare blocks generic/bot UAs
                # and sometimes returns HTML challenge pages with status 200.
                "user-agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/120.0.0.0 Safari/537.36"),
                "accept": "application/json",
                "accept-language": "en-US,en;q=0.9",
            },
        )

    async def close(self) -> None:
        await self.http.aclose()

    async def get_event_by_slug(self, slug: str) -> Optional[dict]:
        try:
            r = await self.http.get(f"{GAMMA_BASE_URL}/events", params={"slug": slug})
            if r.status_code != 200:
                log.warning("gamma fetch %s → status %d", slug, r.status_code)
                return None
            try:
                data = r.json()
            except Exception:
                # Cloudflare sometimes returns HTML with status 200
                snippet = r.text[:200].replace("\n", " ")
                log.warning("gamma fetch %s → non-JSON response: %s", slug, snippet)
                return None
            return data[0] if isinstance(data, list) and data else None
        except Exception as exc:
            log.warning("gamma fetch %s exception: %s", slug, exc)
            return None


def current_window_ts(now: Optional[int] = None) -> int:
    if now is None:
        now = int(time.time())
    return (now // 300) * 300


async def fetch_market(client: PolyClient, coin: str, window_ts: int) -> Optional[CryptoMarket]:
    slug = f"{coin}-updown-5m-{window_ts}"
    evt = await client.get_event_by_slug(slug)
    if not evt or not evt.get("markets"):
        return None
    m = evt["markets"][0]
    if m.get("closed"):
        return None
    try:
        token_ids = json.loads(m.get("clobTokenIds") or "[]")
    except Exception:
        return None
    if len(token_ids) < 2:
        return None
    return CryptoMarket(
        coin=coin,
        window_ts=window_ts,
        slug=slug,
        event_id=str(evt.get("id") or ""),
        condition_id=str(m.get("conditionId") or m.get("condition_id") or ""),
        up_token_id=str(token_ids[0]),
        down_token_id=str(token_ids[1]),
        min_tick_size=str(m.get("minimumTickSize") or "0.01"),
        neg_risk=bool(m.get("negRisk", False)),
    )


# ─── execution (unchanged from previous version) ──────────────────────────
class ExecutionError(RuntimeError):
    pass


_TICK_DECIMALS = {"0.1": 1, "0.01": 2, "0.001": 3, "0.0001": 4}


def _tick_for_price(price: float, market_min_tick: str = "0.01") -> str:
    try:
        m = float(market_min_tick) if market_min_tick else 0.01
    except (TypeError, ValueError):
        m = 0.01
    if (price >= 0.95 or price <= 0.05) and m <= 0.001:
        return "0.001"
    if m >= 0.1:
        return "0.1"
    return "0.01"


def _round_to_tick(price: float, tick: str) -> float:
    decimals = _TICK_DECIMALS.get(tick, 2)
    step = float(tick)
    n = int(price / step + 1e-9)
    p = round(n * step, decimals)
    p = max(step, min(1.0 - step, p))
    return round(p, decimals)


class BaseExecutor:
    def __init__(self, persistence: Persistence) -> None:
        self.persistence = persistence

    async def buy(self, *, market: CryptoMarket, side: str, size_usd: float,
                  ask_price: float) -> Position:
        raise NotImplementedError

    async def place_limit(self, *, market: CryptoMarket, side: str,
                          size_usd: float, limit_price: float) -> dict:
        """Place a GTC limit BUY at limit_price. Returns dict with order_id
        and shares (size_usd / limit_price). Order rests in book."""
        raise NotImplementedError

    async def place_limit_sell(self, *, market: CryptoMarket, side: str,
                               shares: float, limit_price: float) -> dict:
        """Place a GTC limit SELL of `shares` at limit_price. Used to
        offload shares we previously bought."""
        raise NotImplementedError

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a resting order by ID. Returns True if cancelled."""
        raise NotImplementedError


class DryRunExecutor(BaseExecutor):
    async def buy(self, *, market: CryptoMarket, side: str, size_usd: float,
                  ask_price: float) -> Position:
        token_id = market.up_token_id if side == "UP" else market.down_token_id
        shares = size_usd / ask_price if ask_price > 0 else 0.0
        ts = utc_now_iso()
        pos = Position(
            side=side, coin=market.coin, slug=market.slug,
            condition_id=market.condition_id, token_id=token_id,
            entry_price=ask_price, size_usd=size_usd, shares=shares,
            entry_time_iso=ts, neg_risk=market.neg_risk, status="open",
        )
        self.persistence.record_order(
            ts=ts, action="buy_dry", coin=market.coin, slug=market.slug,
            condition_id=market.condition_id, token_id=token_id,
            side=side, price=ask_price, size_usd=size_usd, shares=shares,
            status="dry_run_open",
            payload={"window_ts": market.window_ts, "ask_at_entry": ask_price,
                     "source": "websocket"},
        )
        return pos

    async def place_limit(self, *, market, side, size_usd, limit_price):
        token_id = market.up_token_id if side == "UP" else market.down_token_id
        shares = size_usd / limit_price if limit_price > 0 else 0.0
        ts = utc_now_iso()
        order_id = f"DRY-BUY-{int(time.time()*1000000)}"
        self.persistence.record_order(
            ts=ts, action="limit_dry_buy", coin=market.coin, slug=market.slug,
            condition_id=market.condition_id, token_id=token_id,
            side=side, price=limit_price, size_usd=size_usd, shares=shares,
            status="dry_run_resting", order_id=order_id,
            payload={"window_ts": market.window_ts, "limit_price": limit_price,
                     "type": "GTC", "intent": "buy"},
        )
        return {"order_id": order_id, "shares": shares, "limit_price": limit_price}

    async def place_limit_sell(self, *, market, side, shares, limit_price):
        token_id = market.up_token_id if side == "UP" else market.down_token_id
        ts = utc_now_iso()
        order_id = f"DRY-SELL-{int(time.time()*1000000)}"
        self.persistence.record_order(
            ts=ts, action="limit_dry_sell", coin=market.coin, slug=market.slug,
            condition_id=market.condition_id, token_id=token_id,
            side=side, price=limit_price, size_usd=shares*limit_price, shares=shares,
            status="dry_run_resting", order_id=order_id,
            payload={"window_ts": market.window_ts, "limit_price": limit_price,
                     "type": "GTC", "intent": "sell"},
        )
        return {"order_id": order_id, "shares": shares, "limit_price": limit_price}

    async def cancel_order(self, order_id: str) -> bool:
        return True


class LivePolymarketExecutor(BaseExecutor):
    def __init__(self, persistence: Persistence) -> None:
        super().__init__(persistence)
        self.client = None
        self._neg_risk_cache: dict[str, bool] = {}

    def _ensure_client(self) -> None:
        if self.client is not None:
            return
        try:
            from py_clob_client_v2 import ClobClient, ApiCreds
        except Exception as exc:
            raise ExecutionError(f"py-clob-client-v2 import failed: {exc}") from exc

        funder = POLY_FUNDER
        if not funder and POLY_SIGNATURE_TYPE == 0:
            try:
                from eth_account import Account
                funder = Account.from_key(POLY_PRIVATE_KEY).address
            except Exception:
                funder = ""

        derived = None
        need_derive = POLY_AUTO_DERIVE_CREDS or not (
            POLY_API_KEY and POLY_API_SECRET and POLY_API_PASSPHRASE
        )
        if need_derive:
            tmp = ClobClient(host=POLYMARKET_HOST, chain_id=POLYMARKET_CHAIN_ID,
                             key=POLY_PRIVATE_KEY)
            derived = tmp.create_or_derive_api_key()

        if derived is not None:
            api_key = getattr(derived, "api_key", None) or getattr(derived, "apiKey", None)
            api_secret = getattr(derived, "api_secret", None) or getattr(derived, "secret", None)
            api_passphrase = (getattr(derived, "api_passphrase", None)
                              or getattr(derived, "passphrase", None))
        else:
            api_key, api_secret, api_passphrase = POLY_API_KEY, POLY_API_SECRET, POLY_API_PASSPHRASE

        creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
        # Stash creds on self so user_ws_loop can build its auth payload
        # without depending on V2 SDK exposing them via client attributes.
        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase
        kwargs: dict[str, Any] = dict(
            host=POLYMARKET_HOST, chain_id=POLYMARKET_CHAIN_ID,
            key=POLY_PRIVATE_KEY, creds=creds,
        )
        if POLY_SIGNATURE_TYPE:
            kwargs["signature_type"] = POLY_SIGNATURE_TYPE
        if funder:
            kwargs["funder"] = funder
        self.client = ClobClient(**kwargs)
        log.info("CLOB V2 ready (signature_type=%s funder=%s)", POLY_SIGNATURE_TYPE, funder)

    def _resolve_neg_risk(self, token_id: str, fallback: bool) -> bool:
        if token_id in self._neg_risk_cache:
            return self._neg_risk_cache[token_id]
        try:
            result = self.client.get_neg_risk(token_id)
            if isinstance(result, dict):
                value = bool(result.get("neg_risk", result.get("negRisk", fallback)))
            elif isinstance(result, bool):
                value = result
            else:
                value = bool(getattr(result, "neg_risk",
                                     getattr(result, "negRisk", fallback)))
            self._neg_risk_cache[token_id] = value
            return value
        except Exception as exc:
            log.warning("neg_risk lookup failed (%s) — fallback %s", exc, fallback)
            return fallback

    async def buy(self, *, market: CryptoMarket, side: str, size_usd: float,
                  ask_price: float) -> Position:
        if not is_live_mode():
            raise ExecutionError("live_mode_not_armed")
        token_id = market.up_token_id if side == "UP" else market.down_token_id
        loop = asyncio.get_running_loop()
        # Use a dedicated executor with one worker per concurrent buy so we
        # don't exhaust asyncio's default thread pool with hung HTTP calls.
        if not hasattr(self, "_executor_pool"):
            from concurrent.futures import ThreadPoolExecutor
            self._executor_pool = ThreadPoolExecutor(max_workers=16,
                                                     thread_name_prefix="poly-buy")
        # Hard timeout: SDK has no built-in timeouts, so we enforce one here.
        # 25s = enough for 12s of fill polling + a few HTTP calls + slack.
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(
                    self._executor_pool, self._buy_sync,
                    market, side, token_id, size_usd, ask_price,
                ),
                timeout=25.0,
            )
        except asyncio.TimeoutError:
            raise ExecutionError("buy_timeout (25s) — order may or may not have filled")

    def _buy_sync(self, market: CryptoMarket, side: str, token_id: str,
                  size_usd: float, ask_price: float) -> Position:
        self._ensure_client()
        try:
            from py_clob_client_v2 import (
                MarketOrderArgs, OrderType, PartialCreateOrderOptions, Side,
                BalanceAllowanceParams, AssetType,
            )
        except Exception as exc:
            raise ExecutionError(f"py-clob-client-v2 trading import failed: {exc}") from exc

        ref_ask = ask_price

        def build_args():
            order_tick = _tick_for_price(ref_ask, market_min_tick=market.min_tick_size)
            tick_step = float(order_tick)
            desired = ref_ask + (tick_step * 10)
            max_allowed = round(1.0 - tick_step, _TICK_DECIMALS.get(order_tick, 2))
            order_price = _round_to_tick(min(desired, max_allowed), order_tick)
            actual_neg_risk = self._resolve_neg_risk(token_id, fallback=bool(market.neg_risk))
            return order_tick, order_price, MarketOrderArgs(
                token_id=token_id, amount=float(size_usd), side=Side.BUY,
                order_type=OrderType.FAK, price=order_price,
            ), PartialCreateOrderOptions(tick_size=order_tick, neg_risk=actual_neg_risk)

        balance_before = 0.0
        try:
            pre = self.client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
            )
            raw = pre.get("balance") if isinstance(pre, dict) else None
            if raw is not None:
                balance_before = float(raw) / 1_000_000
        except Exception:
            pass

        response = None
        order_price = None
        for attempt in (1, 2):
            try:
                _tick, order_price, args, options = build_args()
                response = self.client.create_and_post_market_order(
                    order_args=args, options=options, order_type=OrderType.FAK,
                )
                break
            except Exception as exc:
                msg = str(exc).lower()
                if attempt == 1 and "invalid signature" in msg:
                    log.warning("invalid signature attempt 1 — re-deriving creds")
                    self.client = None
                    try:
                        self._ensure_client()
                        continue
                    except Exception as re_exc:
                        raise ExecutionError(f"buy_failed: re-derive: {re_exc}") from re_exc
                raise ExecutionError(f"buy_failed: {type(exc).__name__}: {exc}") from exc

        order_id = None
        order_status = "submitted"
        if isinstance(response, dict):
            order_id = response.get("orderID") or response.get("order_id") or response.get("orderId")
            order_status = str(response.get("status", "submitted"))

        expected_full_shares = size_usd / ref_ask if ref_ask > 0 else 0.0
        fill_threshold = max(0.001, expected_full_shares * 0.10)
        filled = False
        shares_filled = 0.0

        if isinstance(response, dict):
            try:
                taking = float(response.get("takingAmount") or 0.0)
            except (TypeError, ValueError):
                taking = 0.0
            if taking >= fill_threshold:
                shares_filled = taking
                filled = True

        if not filled:
            for i in range(8):
                time.sleep(1.5)
                try:
                    bal = self.client.get_balance_allowance(
                        BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
                    )
                    raw = bal.get("balance") if isinstance(bal, dict) else None
                    if raw is None:
                        continue
                    current = float(raw) / 1_000_000
                    delta = current - balance_before
                    if delta >= fill_threshold:
                        shares_filled = delta
                        filled = True
                        break
                except Exception:
                    pass

        ts = utc_now_iso()
        if not filled:
            self.persistence.record_order(
                ts=ts, action="buy_NOT_FILLED", coin=market.coin, slug=market.slug,
                condition_id=market.condition_id, token_id=token_id, side=side,
                price=ref_ask, size_usd=size_usd, shares=0.0,
                status=order_status, order_id=order_id,
                payload={"order_price": order_price, "ref_ask": ref_ask,
                         "response": response if isinstance(response, dict) else str(response)},
            )
            raise ExecutionError(f"order_not_filled status={order_status} id={order_id}")

        actual_entry = size_usd / shares_filled if shares_filled > 0 else ref_ask
        pos = Position(
            side=side, coin=market.coin, slug=market.slug,
            condition_id=market.condition_id, token_id=token_id,
            entry_price=actual_entry, size_usd=size_usd, shares=shares_filled,
            entry_time_iso=ts, neg_risk=market.neg_risk, order_id=order_id,
            status="open",
        )
        self.persistence.record_order(
            ts=ts, action="buy_FILLED", coin=market.coin, slug=market.slug,
            condition_id=market.condition_id, token_id=token_id, side=side,
            price=actual_entry, size_usd=size_usd, shares=shares_filled,
            status=order_status, order_id=order_id,
            payload={"order_price": order_price, "ref_ask": ref_ask, "source": "websocket"},
        )
        return pos

    async def place_limit(self, *, market: CryptoMarket, side: str,
                          size_usd: float, limit_price: float) -> dict:
        """Place GTC limit BUY at limit_price. Order rests in the book."""
        if not is_live_mode():
            raise ExecutionError("live_mode_not_armed")
        token_id = market.up_token_id if side == "UP" else market.down_token_id
        loop = asyncio.get_running_loop()
        if not hasattr(self, "_executor_pool"):
            from concurrent.futures import ThreadPoolExecutor
            self._executor_pool = ThreadPoolExecutor(max_workers=16,
                                                     thread_name_prefix="poly-buy")
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(
                    self._executor_pool, self._place_limit_sync,
                    market, side, token_id, size_usd, limit_price,
                ),
                timeout=10.0,
            )
        except asyncio.TimeoutError:
            raise ExecutionError("limit_place_timeout (10s)")

    def _place_limit_sync(self, market: CryptoMarket, side: str, token_id: str,
                          size_usd: float, limit_price: float) -> dict:
        self._ensure_client()
        try:
            from py_clob_client_v2 import (
                OrderArgs, OrderType, PartialCreateOrderOptions, Side,
            )
        except Exception as exc:
            raise ExecutionError(f"py-clob-client-v2 trading import failed: {exc}") from exc

        order_tick = _tick_for_price(limit_price, market_min_tick=market.min_tick_size)
        order_price = _round_to_tick(limit_price, order_tick)
        # size in shares = size_usd / limit_price
        shares = size_usd / order_price if order_price > 0 else 0.0
        actual_neg_risk = self._resolve_neg_risk(token_id, fallback=bool(market.neg_risk))

        # GTC limit order — rests in the book until filled or cancelled.
        # OrderArgs.size is in SHARES (not USD).
        args = OrderArgs(
            token_id=token_id,
            price=order_price,
            size=float(shares),
            side=Side.BUY,
        )
        options = PartialCreateOrderOptions(tick_size=order_tick, neg_risk=actual_neg_risk)

        response = None
        for attempt in (1, 2):
            try:
                response = self.client.create_and_post_order(
                    order_args=args, options=options, order_type=OrderType.GTC,
                )
                break
            except Exception as exc:
                msg = str(exc).lower()
                if attempt == 1 and "invalid signature" in msg:
                    log.warning("invalid signature on limit attempt 1 — re-deriving creds")
                    self.client = None
                    try:
                        self._ensure_client()
                        continue
                    except Exception as re_exc:
                        raise ExecutionError(f"limit_failed: re-derive: {re_exc}") from re_exc
                raise ExecutionError(f"limit_failed: {type(exc).__name__}: {exc}") from exc

        order_id = None
        order_status = "submitted"
        if isinstance(response, dict):
            order_id = (response.get("orderID") or response.get("order_id")
                        or response.get("orderId"))
            order_status = str(response.get("status", "submitted"))

        ts = utc_now_iso()
        self.persistence.record_order(
            ts=ts, action="limit_placed", coin=market.coin, slug=market.slug,
            condition_id=market.condition_id, token_id=token_id, side=side,
            price=order_price, size_usd=size_usd, shares=shares,
            status=order_status, order_id=order_id,
            payload={"limit_price": order_price, "type": "GTC",
                     "window_ts": market.window_ts},
        )
        return {"order_id": order_id, "shares": shares,
                "limit_price": order_price, "status": order_status}

    async def place_limit_sell(self, *, market: CryptoMarket, side: str,
                               shares: float, limit_price: float) -> dict:
        """Place GTC limit SELL of `shares` at limit_price."""
        if not is_live_mode():
            raise ExecutionError("live_mode_not_armed")
        token_id = market.up_token_id if side == "UP" else market.down_token_id
        loop = asyncio.get_running_loop()
        if not hasattr(self, "_executor_pool"):
            from concurrent.futures import ThreadPoolExecutor
            self._executor_pool = ThreadPoolExecutor(max_workers=16,
                                                     thread_name_prefix="poly-buy")
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(
                    self._executor_pool, self._place_limit_sell_sync,
                    market, side, token_id, shares, limit_price,
                ),
                timeout=10.0,
            )
        except asyncio.TimeoutError:
            raise ExecutionError("limit_sell_place_timeout (10s)")

    def _place_limit_sell_sync(self, market: CryptoMarket, side: str, token_id: str,
                               shares: float, limit_price: float) -> dict:
        self._ensure_client()
        try:
            from py_clob_client_v2 import (
                OrderArgs, OrderType, PartialCreateOrderOptions, Side,
            )
        except Exception as exc:
            raise ExecutionError(f"py-clob-client-v2 trading import failed: {exc}") from exc

        order_tick = _tick_for_price(limit_price, market_min_tick=market.min_tick_size)
        order_price = _round_to_tick(limit_price, order_tick)
        actual_neg_risk = self._resolve_neg_risk(token_id, fallback=bool(market.neg_risk))

        args = OrderArgs(
            token_id=token_id, price=order_price,
            size=float(shares), side=Side.SELL,
        )
        options = PartialCreateOrderOptions(tick_size=order_tick, neg_risk=actual_neg_risk)

        response = None
        for attempt in (1, 2):
            try:
                response = self.client.create_and_post_order(
                    order_args=args, options=options, order_type=OrderType.GTC,
                )
                break
            except Exception as exc:
                msg = str(exc).lower()
                if attempt == 1 and "invalid signature" in msg:
                    self.client = None
                    try:
                        self._ensure_client()
                        continue
                    except Exception as re_exc:
                        raise ExecutionError(f"sell_failed: re-derive: {re_exc}") from re_exc
                raise ExecutionError(f"sell_failed: {type(exc).__name__}: {exc}") from exc

        order_id = None
        order_status = "submitted"
        if isinstance(response, dict):
            order_id = (response.get("orderID") or response.get("order_id")
                        or response.get("orderId"))
            order_status = str(response.get("status", "submitted"))

        ts = utc_now_iso()
        self.persistence.record_order(
            ts=ts, action="limit_sell_placed", coin=market.coin, slug=market.slug,
            condition_id=market.condition_id, token_id=token_id, side=side,
            price=order_price, size_usd=shares*order_price, shares=shares,
            status=order_status, order_id=order_id,
            payload={"limit_price": order_price, "type": "GTC", "intent": "sell",
                     "window_ts": market.window_ts},
        )
        return {"order_id": order_id, "shares": shares,
                "limit_price": order_price, "status": order_status}

    async def cancel_order(self, order_id: str) -> bool:
        if not is_live_mode():
            return True
        loop = asyncio.get_running_loop()
        if not hasattr(self, "_executor_pool"):
            from concurrent.futures import ThreadPoolExecutor
            self._executor_pool = ThreadPoolExecutor(max_workers=16,
                                                     thread_name_prefix="poly-buy")
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(self._executor_pool, self._cancel_sync, order_id),
                timeout=10.0,
            )
        except asyncio.TimeoutError:
            log.warning("cancel timeout for %s", order_id)
            return False

    def _cancel_sync(self, order_id: str) -> bool:
        self._ensure_client()
        try:
            # V2 SDK uses cancel_order(order_id=...). Some versions accept just
            # a positional arg or use bulk cancel_orders.
            result = None
            for attempt_method in ("cancel_order", "cancel_orders"):
                if hasattr(self.client, attempt_method):
                    method = getattr(self.client, attempt_method)
                    if attempt_method == "cancel_orders":
                        result = method([order_id])
                    else:
                        try:
                            result = method(order_id=order_id)
                        except TypeError:
                            result = method(order_id)
                    break
            if result is None:
                log.warning("no cancel method found on ClobClient")
                return False
            ts = utc_now_iso()
            self.persistence.record_order(
                ts=ts, action="cancel", coin="", slug="",
                condition_id="", token_id="", side="",
                price=0.0, size_usd=0.0, shares=0.0,
                status="cancelled", order_id=order_id,
                payload={"result": result if isinstance(result, dict) else str(result)},
            )
            return True
        except Exception as exc:
            log.warning("cancel failed for %s: %s", order_id, exc)
            return False


# ─── bot ──────────────────────────────────────────────────────────────────
class Bot:
    def __init__(self) -> None:
        self.persistence = Persistence(DB_PATH)
        self.client = PolyClient()
        if is_live_mode():
            self.executor: BaseExecutor = LivePolymarketExecutor(self.persistence)
            log.warning("⚡ LIVE MODE ARMED — real money at risk")
        else:
            self.executor = DryRunExecutor(self.persistence)
            log.info("🧪 DRY-RUN MODE")

        self.markets: dict[str, CryptoMarket] = {}
        self.books: dict[str, TokenBook] = {}
        self.token_lookup: dict[str, tuple[CryptoMarket, str]] = {}
        # Per-rung trigger tracking: (slug, side, rung_idx) → fired
        self.triggered: set[tuple[str, str, int]] = set()
        # Per-rung seen-below tracking. Each rung needs its own confirmation
        # that the ask was below it before allowing trigger.
        self.seen_below: set[tuple[str, str, int]] = set()
        self.subscription_changed = asyncio.Event()
        self.start_time = time.time()

        # Track BUY orders by slug for cancellation at boundary.
        # slug → list[order_id]
        self.buy_orders_by_slug: dict[str, list[str]] = {}
        # Track which (slug, side, price) we've already placed a BUY for —
        # prevents duplicate orders if new_market arrives multiple times or
        # if discovery loop races with new_market event.
        self.buys_placed: set[tuple[str, str, float]] = set()
        # Track fills we've already responded to with a sell, to prevent
        # placing duplicate sells if user-channel sends repeat events.
        self.sells_placed_for_order: set[str] = set()
        # Slugs we've fully processed (placed orders for). Prevents re-placing
        # if new_market and discovery_loop both find the same slug.
        self.markets_initialized: set[str] = set()

    async def discovery_loop(self) -> None:
        """Always work on the NEXT market (the one after current window).
        The next market already exists on Polymarket the moment the current
        one starts, so we can fetch it and place the buy ladder immediately —
        giving us up to 5 minutes of pre-market resting orders.

        Loop logic: every 30 seconds, ensure we have the upcoming window
        loaded. When boundaries cross, the upcoming becomes current, and the
        loop discovers the next-upcoming."""
        log.info("discovery: continuous next-window discovery (5min lookahead)")
        iteration = 0

        while True:
            try:
                iteration += 1
                now = time.time()
                # The "current" window is the one we're inside right now
                current_window = (int(now) // 300) * 300
                # The "next" window is the one after current
                next_window = current_window + 300
                if iteration <= 3 or iteration % 10 == 0:
                    log.info("discovery iter %d: current=%d next=%d markets_tracked=%d",
                             iteration, current_window, next_window, len(self.markets))

                # Expire markets older than current
                expired_slugs = [s for s, m in list(self.markets.items())
                                 if m.window_ts < current_window]
                for s in expired_slugs:
                    m = self.markets.pop(s, None)
                    if m:
                        self.books.pop(m.up_token_id, None)
                        self.books.pop(m.down_token_id, None)
                        self.token_lookup.pop(m.up_token_id, None)
                        self.token_lookup.pop(m.down_token_id, None)

                # Discover BOTH windows in parallel: the currently-running one
                # (in case we just started up) AND the next one (for pre-market
                # ladder placement). Idempotent — _on_market_discovered skips
                # already-initialized slugs.
                target_windows = [current_window, next_window]
                tasks = []
                for window in target_windows:
                    for coin in COINS:
                        # Skip coins where we already have a market for this window
                        slug_expected = f"{coin}-updown-5m-{window}"
                        if slug_expected in self.markets_initialized:
                            continue
                        tasks.append((coin, window,
                                      fetch_market(self.client, coin, window)))

                if tasks:
                    results = await asyncio.gather(
                        *(t[2] for t in tasks), return_exceptions=True,
                    )
                    for (coin, window, _), m in zip(tasks, results):
                        if isinstance(m, Exception) or m is None:
                            continue
                        if m.slug in self.markets_initialized:
                            continue
                        await self._on_market_discovered(m, attempt=1)

                self.subscription_changed.set()

                # Sleep until either:
                # (a) the next-window slug should exist (next boundary - 30s)
                # (b) at most 30s, so we don't drift if Polymarket is slow
                now2 = time.time()
                sleep_until = min(next_window - 30, now2 + 30)
                wait = max(1.0, sleep_until - now2)
                await asyncio.sleep(wait)

            except Exception as exc:
                log.exception("discovery error: %s", exc)
                await asyncio.sleep(5.0)

    async def _on_market_discovered(self, m: CryptoMarket, attempt: int) -> None:
        """Called when a market is found via Gamma poll. Registers it and (if
        STRATEGY=limits) places the BUY ladder. Idempotent: won't re-place if
        the new_market WS event already handled this slug."""
        if m.slug in self.markets_initialized:
            return
        self.markets_initialized.add(m.slug)

        latency_ms = (time.time() - m.window_ts) * 1000
        self.markets[m.slug] = m
        self.books[m.up_token_id] = TokenBook()
        self.books[m.down_token_id] = TokenBook()
        self.token_lookup[m.up_token_id] = (m, "UP")
        self.token_lookup[m.down_token_id] = (m, "DOWN")
        debug.event(f"discovered {m.slug} ({m.seconds_left()}s left, "
                    f"+{latency_ms:.0f}ms after open, source=gamma attempt {attempt})")

        if STRATEGY == "limits":
            await self._fire_buy_ladder(m, source="gamma")

    async def _handle_new_market(self, msg: dict) -> None:
        """Handle a 'new_market' WS event — Polymarket pushes this the
        moment a market is created. Includes clob_token_ids, so we can
        fire orders without waiting for Gamma to index. This is the
        zero-latency path for catching pre-market windows."""
        try:
            slug = str(msg.get("slug") or "")
            if not slug:
                return
            # Filter for our coins + 5m markets only
            if "-updown-5m-" not in slug:
                return
            coin = slug.split("-updown-5m-")[0]
            if coin not in COINS:
                return
            try:
                window_ts = int(slug.rsplit("-", 1)[-1])
            except ValueError:
                return

            if slug in self.markets_initialized:
                return  # already handled by Gamma poll

            # Filter: Polymarket pre-creates 5m markets hours/days ahead. Only act
            # on the current or next-upcoming window. 600s = 10min covers current
            # plus the immediately-upcoming window with margin.
            now = time.time()
            secs_until_open = window_ts - now
            if secs_until_open > 600:
                # Too far in the future — Polymarket bulk-publishing
                return
            if secs_until_open < -60:
                # Too far in the past
                return

            token_ids = msg.get("clob_token_ids") or msg.get("assets_ids") or []
            if len(token_ids) < 2:
                debug.event(f"new_market {slug} missing token_ids: {msg}")
                return
            condition_id = str(msg.get("condition_id") or "")

            m = CryptoMarket(
                coin=coin, window_ts=window_ts, slug=slug,
                event_id=str(msg.get("event_id") or ""),
                condition_id=condition_id,
                up_token_id=str(token_ids[0]),
                down_token_id=str(token_ids[1]),
                min_tick_size=str(msg.get("order_price_min_tick_size") or "0.01"),
                neg_risk=bool(msg.get("neg_risk", False)),
            )

            self.markets_initialized.add(slug)
            self.markets[slug] = m
            self.books[m.up_token_id] = TokenBook()
            self.books[m.down_token_id] = TokenBook()
            self.token_lookup[m.up_token_id] = (m, "UP")
            self.token_lookup[m.down_token_id] = (m, "DOWN")
            self.subscription_changed.set()

            debug.event(f"NEW_MARKET ws-event {slug} ({secs_until_open:+.1f}s to open) "
                        f"— firing pre-market orders")

            if STRATEGY == "limits":
                await self._fire_buy_ladder(m, source="ws_new_market")
        except Exception as exc:
            log.exception("new_market handler error: %s", exc)

    async def _fire_buy_ladder(self, m: CryptoMarket, source: str) -> None:
        """Place BUY limits at LIMIT_BUY_PRICES on both sides, concurrently.
        Records order IDs in self.buy_orders_by_slug for later cancellation."""
        tasks = []
        for side in ("UP", "DOWN"):
            for buy_price in LIMIT_BUY_PRICES:
                tasks.append(self._place_one_buy(m, side, buy_price, source))
        await asyncio.gather(*tasks, return_exceptions=True)
        # Schedule a one-shot task to cancel any unfilled BUY orders for
        # this slug shortly after the window opens.
        asyncio.create_task(self._cancel_buys_at_open(m))

    async def _place_one_buy(self, market: CryptoMarket, side: str,
                             buy_price: float, source: str) -> None:
        # Dedup: if we've already placed this exact buy for this slug+side+price, skip
        key = (market.slug, side, buy_price)
        if key in self.buys_placed:
            return
        self.buys_placed.add(key)

        try:
            result = await self.executor.place_limit(
                market=market, side=side,
                size_usd=POSITION_SIZE_USD, limit_price=buy_price,
            )
            order_id = result.get("order_id")
            if order_id:
                self.buy_orders_by_slug.setdefault(market.slug, []).append(order_id)
            debug.event(f"BUY placed {market.slug} {side} @ {buy_price} "
                        f"({result.get('shares', 0):.4f} sh, id={order_id}, src={source})")
        except ExecutionError as exc:
            self.buys_placed.discard(key)  # allow retry
            debug.event(f"BUY failed {market.slug} {side} @ {buy_price}: {exc}")
        except Exception as exc:
            self.buys_placed.discard(key)
            log.exception("buy UNEXPECTED %s %s @ %s: %s",
                          market.slug, side, buy_price, exc)

    async def _cancel_buys_at_open(self, m: CryptoMarket) -> None:
        """Wait until the window opens (+ small grace period), then cancel
        any unfilled BUY orders for this slug."""
        wait_until = m.window_ts + CANCEL_BUYS_AT_OPEN_DELAY
        wait = wait_until - time.time()
        if wait > 0:
            await asyncio.sleep(wait)

        order_ids = self.buy_orders_by_slug.get(m.slug, [])
        if not order_ids:
            return
        debug.event(f"cancelling {len(order_ids)} unfilled BUYs for {m.slug}")
        # Fire all cancels concurrently
        tasks = [self.executor.cancel_order(oid) for oid in order_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        cancelled = sum(1 for r in results if r is True)
        debug.event(f"cancelled {cancelled}/{len(order_ids)} BUYs for {m.slug}")
        # Clear the list — anything that fills after this point can't be cancelled
        # because Polymarket will reject cancel on filled orders (which is fine).
        self.buy_orders_by_slug.pop(m.slug, None)

    async def _handle_fill(self, fill_msg: dict) -> None:
        """Called when user-channel WS pushes a trade event indicating a fill.
        If the fill was on one of our BUY orders, immediately place the
        corresponding SELL @ LIMIT_SELL_PRICE."""
        try:
            order_id = str(fill_msg.get("order_id") or fill_msg.get("orderID") or
                           fill_msg.get("maker_order_id") or "")
            if not order_id:
                return
            if order_id in self.sells_placed_for_order:
                return  # already responded

            # Was this one of OUR buy orders?
            slug_match = None
            for slug, ids in self.buy_orders_by_slug.items():
                if order_id in ids:
                    slug_match = slug
                    break
            if not slug_match:
                # Not one of our tracked buys — could be a sell filling, or
                # an order from before bot restart. Ignore.
                return

            market = self.markets.get(slug_match)
            if market is None:
                return

            # Determine side from token ID in the fill message
            token_id = str(fill_msg.get("asset_id") or fill_msg.get("token_id") or "")
            side = None
            if token_id == market.up_token_id:
                side = "UP"
            elif token_id == market.down_token_id:
                side = "DOWN"
            if side is None:
                return

            # Get filled size
            try:
                shares_filled = float(fill_msg.get("size") or
                                      fill_msg.get("matched_amount") or 0)
            except (TypeError, ValueError):
                shares_filled = 0.0
            if shares_filled < 1.0:
                # Polymarket min order is 5 shares. Below 1 likely a partial
                # we should aggregate, but for simplicity we skip here.
                return

            self.sells_placed_for_order.add(order_id)
            debug.event(f"FILL detected {slug_match} {side} order={order_id[:16]}… "
                        f"({shares_filled:.4f} sh) — placing SELL @ {LIMIT_SELL_PRICE}")

            try:
                result = await self.executor.place_limit_sell(
                    market=market, side=side,
                    shares=shares_filled, limit_price=LIMIT_SELL_PRICE,
                )
                debug.event(f"SELL placed {slug_match} {side} @ {LIMIT_SELL_PRICE} "
                            f"({shares_filled:.4f} sh, id={result.get('order_id')})")
            except ExecutionError as exc:
                debug.event(f"SELL failed {slug_match} {side}: {exc}")
            except Exception as exc:
                log.exception("sell UNEXPECTED %s %s: %s", slug_match, side, exc)
        except Exception as exc:
            log.exception("fill handler error: %s", exc)


    async def websocket_loop(self) -> None:
        backoff = 1.0
        while True:
            try:
                # Don't block on empty token list — we want to receive new_market
                # events even before we have any markets yet.
                token_ids = list(self.token_lookup.keys())
                debug.connect_count += 1
                debug.last_ws_connect_at = time.time()
                debug.event(f"WS connecting ({len(token_ids)} tokens)")

                async with websockets.connect(
                    CLOB_WS_URL,
                    ping_interval=10,    # was 20 — faster heartbeat
                    ping_timeout=5,      # was 10
                    close_timeout=5,
                ) as ws:
                    backoff = 1.0
                    # Polymarket CLOB market WS subscribe shape.
                    # custom_feature_enabled=true unlocks new_market and best_bid_ask events.
                    sub_msg = {
                        "assets_ids": token_ids,
                        "type": "market",
                        "custom_feature_enabled": True,
                    }
                    sub_raw = json.dumps(sub_msg)
                    await ws.send(sub_raw)
                    debug.ws_msg(sub_raw, kind="sub")
                    debug.event(f"WS subscribed to {len(token_ids)} tokens")

                    self.subscription_changed.clear()
                    recv_task = asyncio.create_task(self._ws_recv(ws))
                    sub_task = asyncio.create_task(self.subscription_changed.wait())

                    done, pending = await asyncio.wait(
                        {recv_task, sub_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for t in pending:
                        t.cancel()
                        with suppress(asyncio.CancelledError):
                            await t

                    if sub_task in done:
                        debug.event("WS subscription set changed → reconnecting")
                    else:
                        exc = recv_task.exception()
                        if exc:
                            debug.event(f"WS recv ended: {exc}")
                        else:
                            debug.event("WS recv ended cleanly")
            except Exception as exc:
                debug.event(f"WS loop error: {exc} — backoff {backoff:.1f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _ws_recv(self, ws) -> None:
        async for raw in ws:
            try:
                debug.ws_msg(raw if isinstance(raw, str) else raw.decode("utf-8"))
                msg = json.loads(raw)
            except Exception as exc:
                debug.event(f"WS parse error: {exc}")
                continue

            messages = msg if isinstance(msg, list) else [msg]
            for m in messages:
                if not isinstance(m, dict):
                    continue
                event_type = (m.get("event_type") or m.get("type") or "").lower()

                if event_type == "book":
                    self._handle_book_snapshot(m)
                elif event_type == "price_change":
                    self._handle_price_change(m)
                elif event_type == "new_market":
                    # NEW: Polymarket pushes this when a market is created.
                    # Includes clob_token_ids — we can place orders immediately
                    # without waiting for Gamma to index.
                    asyncio.create_task(self._handle_new_market(m))
                elif event_type in ("last_trade_price", "tick_size_change",
                                    "best_bid_ask", "market_resolved", ""):
                    pass
                else:
                    debug.unknown_event_types[event_type] = (
                        debug.unknown_event_types.get(event_type, 0) + 1
                    )

            await self._check_all_triggers()

    def _handle_book_snapshot(self, msg: dict) -> None:
        token_id = str(msg.get("asset_id") or msg.get("market") or "")
        if not token_id or token_id not in self.books:
            return

        def parse_levels(key: str) -> list[tuple[float, float]]:
            raw = msg.get(key) or []
            out = []
            for lvl in raw:
                try:
                    out.append((float(lvl.get("price")), float(lvl.get("size"))))
                except (TypeError, ValueError):
                    continue
            return out

        asks = parse_levels("asks")
        bids = parse_levels("bids")
        book = self.books[token_id]
        book.replace_book(asks, bids)
        debug.last_book_snapshot_at = time.time()

    def _handle_price_change(self, msg: dict) -> None:
        token_id = str(msg.get("asset_id") or msg.get("market") or "")
        if not token_id or token_id not in self.books:
            return
        changes_raw = msg.get("changes") or msg.get("price_changes") or []
        ask_changes: list[tuple[float, float]] = []
        bid_changes: list[tuple[float, float]] = []
        for ch in changes_raw:
            side_str = str(ch.get("side", "")).lower()
            try:
                price = float(ch.get("price"))
                size = float(ch.get("size"))
            except (TypeError, ValueError):
                continue
            if side_str in ("sell", "ask", "asks"):
                ask_changes.append((price, size))
            elif side_str in ("buy", "bid", "bids"):
                bid_changes.append((price, size))
        book = self.books[token_id]
        if ask_changes:
            book.apply_changes("ask", ask_changes)
        if bid_changes:
            book.apply_changes("bid", bid_changes)
        debug.last_price_change_at = time.time()

    async def _check_all_triggers(self) -> None:
        # In limits mode, all entries are placed at window discovery — no
        # cross-the-ask logic needed. WS is still useful for status display.
        if STRATEGY == "limits":
            return
        for token_id, book in self.books.items():
            if token_id not in self.token_lookup:
                continue
            ask = book.best_ask()
            if ask is None:
                continue
            market, side = self.token_lookup[token_id]
            await self._maybe_buy(market, side, ask)

    async def user_ws_loop(self) -> None:
        """User-channel WS for instant fill detection. The moment a BUY fills,
        place SELL @ LIMIT_SELL_PRICE. If both UP and DOWN have shares, merge
        them to USDC for guaranteed profit (UP+DOWN pair = $1.00)."""
        log.info("user_ws: starting")
        if not is_live_mode():
            log.info("user_ws: dry-run mode, skipping")
            return
        if not isinstance(self.executor, LivePolymarketExecutor):
            log.info("user_ws: not LivePolymarketExecutor, skipping")
            return

        # Track shares we've already placed sells for (token_id → shares)
        if not hasattr(self, "shares_sold_for_token"):
            self.shares_sold_for_token: dict[str, float] = {}
        # Track shares already merged (slug → shares merged) so we don't double-merge
        if not hasattr(self, "shares_merged_for_slug"):
            self.shares_merged_for_slug: dict[str, float] = {}

        url = CLOB_WS_URL.replace("/ws/market", "/ws/user")
        backoff = 1.0
        while True:
            try:
                # Force executor to derive creds if not yet
                if self.executor.client is None:
                    log.info("user_ws: forcing executor init for creds")
                    self.executor._ensure_client()

                api_key = getattr(self.executor, "_api_key", None)
                api_secret = getattr(self.executor, "_api_secret", None)
                api_passphrase = getattr(self.executor, "_api_passphrase", None)

                if not (api_key and api_secret and api_passphrase):
                    log.warning("user_ws: missing creds — retry 5s "
                                "(key=%s secret=%s pass=%s)",
                                bool(api_key), bool(api_secret), bool(api_passphrase))
                    await asyncio.sleep(5)
                    continue

                log.info("user_ws: connecting to %s", url)
                async with websockets.connect(url, ping_interval=10, ping_timeout=5,
                                              close_timeout=5) as ws:
                    backoff = 1.0
                    # Per Polymarket docs: omit `markets` to receive ALL events for our key.
                    # Better than subscribing to specific conditions because new markets
                    # appear constantly — we'd miss fills on markets we hadn't subscribed to.
                    sub_msg = {
                        "auth": {
                            "apiKey": api_key,
                            "secret": api_secret,
                            "passphrase": api_passphrase,
                        },
                        "type": "user",
                    }
                    await ws.send(json.dumps(sub_msg))
                    log.info("user_ws: subscribed (all markets)")
                    debug.event("user_ws subscribed (all markets)")

                    async for raw in ws:
                        try:
                            decoded = raw if isinstance(raw, str) else raw.decode("utf-8")
                            if decoded.strip() == "PONG":
                                continue
                            debug.ws_msg(f"[user] {decoded[:600]}", kind="in")
                            msg = json.loads(decoded)
                        except Exception as exc:
                            log.warning("user_ws parse error: %s", exc)
                            continue
                        messages = msg if isinstance(msg, list) else [msg]
                        for m in messages:
                            if not isinstance(m, dict):
                                continue
                            etype = (m.get("event_type") or m.get("type") or "").lower()
                            log.info("user_ws event: %s", etype or "?")
                            # MATCHED = trade matched in CLOB. CONFIRMED = on-chain.
                            # We act on MATCHED for speed (don't wait for chain).
                            if etype == "trade":
                                status = str(m.get("status", "")).lower()
                                if status in ("matched", "mined", "confirmed"):
                                    await self._handle_user_event(m)
                            elif etype == "order":
                                # Order lifecycle event — could indicate fill
                                status = str(m.get("status", "")).lower()
                                if status in ("matched", "filled", "partial_fill"):
                                    await self._handle_user_event(m)
            except Exception as exc:
                log.warning("user_ws error: %s — backoff %.1fs", exc, backoff)
                debug.event(f"user_ws error: {exc} — backoff {backoff:.1f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _handle_user_event(self, ev: dict) -> None:
        """Called on every user-channel trade/order event. Determines which
        of our markets/sides this event affects, places SELL on fills, and
        triggers merge when both UP and DOWN are held."""
        try:
            # The event may be a trade or order lifecycle. Either way, find the
            # token_id of the asset that was matched.
            token_id = str(ev.get("asset_id") or ev.get("token_id")
                           or ev.get("maker_asset_id") or ev.get("taker_asset_id") or "")
            if not token_id or token_id not in self.token_lookup:
                # Not one of our tracked markets
                return

            market, side = self.token_lookup[token_id]

            # Determine the side of the fill — only act on BUY fills
            ev_side = str(ev.get("side", "")).lower()
            # On user channel, side is from our perspective. BUY fill → we received shares.
            if ev_side and ev_side != "buy":
                return  # we don't auto-action on sells/own sells filling

            # Get filled size — try common field names
            shares_filled = 0.0
            for key in ("size", "matched_amount", "size_matched", "filled_size",
                        "maker_amount_filled"):
                v = ev.get(key)
                if v is not None:
                    try:
                        shares_filled = float(v)
                        break
                    except (TypeError, ValueError):
                        pass
            if shares_filled < 1.0:
                # Could be 0 from a partial event we already processed
                return

            log.info("FILL %s %s: %.4f sh from event", market.slug, side, shares_filled)
            debug.event(f"FILL ws {market.slug} {side} ({shares_filled:.4f} sh)")

            # 1) Place SELL @ LIMIT_SELL_PRICE for these new shares
            sold_already = self.shares_sold_for_token.get(token_id, 0.0)
            self.shares_sold_for_token[token_id] = sold_already + shares_filled
            try:
                result = await self.executor.place_limit_sell(
                    market=market, side=side,
                    shares=shares_filled, limit_price=LIMIT_SELL_PRICE,
                )
                debug.event(f"SELL placed {market.slug} {side} @ {LIMIT_SELL_PRICE} "
                            f"({shares_filled:.4f} sh, id={result.get('order_id')})")
            except ExecutionError as exc:
                debug.event(f"SELL failed {market.slug} {side}: {exc}")
                self.shares_sold_for_token[token_id] = sold_already  # rollback
            except Exception as exc:
                log.exception("sell placement error: %s", exc)
                self.shares_sold_for_token[token_id] = sold_already

            # 2) MERGE: if we now hold BOTH UP and DOWN shares for this slug,
            #    convert pairs to USDC for risk-free profit.
            await self._maybe_merge_pair(market)
        except Exception as exc:
            log.exception("user event handler error: %s", exc)

    async def _maybe_merge_pair(self, market: "CryptoMarket") -> None:
        """If we hold both UP and DOWN shares for a market, merge the
        overlapping pairs back to USDC (each pair = $1.00). Only the
        minimum of the two sides can be merged."""
        try:
            up_bal = await self._get_token_balance(market.up_token_id)
            down_bal = await self._get_token_balance(market.down_token_id)
            pair_count = min(up_bal, down_bal)
            already_merged = self.shares_merged_for_slug.get(market.slug, 0.0)
            net_to_merge = pair_count - already_merged
            if net_to_merge < 5.0:  # Polymarket min unit
                return

            log.info("MERGE %s: UP=%.4f DOWN=%.4f → merging %.4f pairs",
                     market.slug, up_bal, down_bal, net_to_merge)
            debug.event(f"MERGE {market.slug} {net_to_merge:.4f} pairs (UP+DOWN → USDC)")
            self.shares_merged_for_slug[market.slug] = already_merged + net_to_merge
            try:
                result = await self._merge_positions(market, net_to_merge)
                debug.event(f"MERGE done {market.slug}: {result}")
            except Exception as exc:
                log.exception("merge failed %s: %s", market.slug, exc)
                debug.event(f"MERGE failed {market.slug}: {exc}")
                self.shares_merged_for_slug[market.slug] = already_merged
        except Exception as exc:
            log.exception("merge check error: %s", exc)

    async def _merge_positions(self, market: "CryptoMarket", shares: float) -> dict:
        """Call Polymarket's merge to convert UP+DOWN pairs back to USDC."""
        loop = asyncio.get_running_loop()
        if not hasattr(self.executor, "_executor_pool"):
            from concurrent.futures import ThreadPoolExecutor
            self.executor._executor_pool = ThreadPoolExecutor(
                max_workers=16, thread_name_prefix="poly-buy")
        return await asyncio.wait_for(
            loop.run_in_executor(
                self.executor._executor_pool, self._merge_positions_sync, market, shares),
            timeout=15.0,
        )

    def _merge_positions_sync(self, market: "CryptoMarket", shares: float) -> dict:
        """Synchronously execute the merge. The V2 SDK exposes this differently
        depending on version — try a few likely method names."""
        client = self.executor.client
        condition_id = market.condition_id
        # Polymarket uses 6-decimal scaling for amounts in some methods
        amount_raw = int(shares * 1_000_000)

        # Try several method signatures the V2 SDK might use
        attempts = [
            ("merge_positions", {"condition_id": condition_id,
                                 "amount": shares,
                                 "neg_risk": bool(market.neg_risk)}),
            ("merge",          {"condition_id": condition_id,
                                 "amount": shares}),
            ("merge_positions",{"condition_id": condition_id, "amount": amount_raw}),
        ]
        last_exc = None
        for method_name, kwargs in attempts:
            if not hasattr(client, method_name):
                continue
            try:
                method = getattr(client, method_name)
                return {"method": method_name, "result": method(**kwargs)}
            except TypeError as exc:
                last_exc = exc
                continue
            except Exception as exc:
                last_exc = exc
                # If the method exists but errors, no point trying others
                raise

        # Fallback: direct on-chain call to ConditionalTokens contract
        log.warning("no SDK merge method found; merge may need manual on-chain call")
        if last_exc:
            raise last_exc
        raise ExecutionError("merge: no compatible SDK method found")

    async def _get_token_balance(self, token_id: str) -> float:
        """Return shares owned for a given token_id."""
        loop = asyncio.get_running_loop()
        if not hasattr(self.executor, "_executor_pool"):
            from concurrent.futures import ThreadPoolExecutor
            self.executor._executor_pool = ThreadPoolExecutor(
                max_workers=16, thread_name_prefix="poly-buy")
        return await asyncio.wait_for(
            loop.run_in_executor(
                self.executor._executor_pool, self._get_token_balance_sync, token_id),
            timeout=10.0,
        )

    def _get_token_balance_sync(self, token_id: str) -> float:
        try:
            from py_clob_client_v2 import BalanceAllowanceParams, AssetType
        except Exception:
            return 0.0
        try:
            res = self.executor.client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
            )
            raw = res.get("balance") if isinstance(res, dict) else None
            if raw is None:
                return 0.0
            return float(raw) / 1_000_000
        except Exception as exc:
            log.warning("balance query failed: %s", exc)
            return 0.0

    async def resolution_loop(self) -> None:
        while True:
            try:
                opens = self.persistence.open_positions()
                slugs = {p["slug"] for p in opens}
                for slug in slugs:
                    try:
                        ts_part = int(slug.rsplit("-", 1)[-1])
                    except Exception:
                        continue
                    if int(time.time()) < ts_part + 360:
                        continue
                    evt = await self.client.get_event_by_slug(slug)
                    if not evt or not evt.get("markets"):
                        continue
                    m = evt["markets"][0]
                    if not m.get("closed"):
                        continue
                    try:
                        outcome_prices = json.loads(m.get("outcomePrices") or "[]")
                    except Exception:
                        continue
                    if len(outcome_prices) < 2:
                        continue
                    up_won = float(outcome_prices[0]) >= 0.99
                    for p in (pp for pp in opens if pp["slug"] == slug):
                        won = (p["side"] == "UP" and up_won) or (p["side"] == "DOWN" and not up_won)
                        gross = p["shares"] if won else 0.0
                        pnl = gross - p["size_usd"]
                        reason = "win" if won else "loss"
                        self.persistence.update_position_resolved(p["id"], pnl, reason)
                        debug.event(f"{'✓' if won else '✗'} {p['side']} {p['slug']}: "
                                    f"{pnl:+.4f} (entry {p['entry_price']:.4f} × {p['shares']:.4f})")
            except Exception as exc:
                log.exception("resolution loop error: %s", exc)
            await asyncio.sleep(15)

    async def status_loop(self) -> None:
        while True:
            try:
                s = self.persistence.stats_today()
                wins = s.get("wins") or 0
                losses = s.get("losses") or 0
                closed = wins + losses
                wr = (wins / closed * 100) if closed else 0.0
                ws_age = (time.time() - debug.last_ws_message_at) if debug.last_ws_message_at else None
                ws_age_s = f"{ws_age:.0f}s ago" if ws_age is not None else "never"
                log.info("📊 %d open · %dW %dL %.1f%% · pnl %+.2f · markets %d · ws msgs %d (last %s)",
                         s.get("open_n") or 0, wins, losses, wr,
                         s.get("pnl") or 0.0, len(self.markets),
                         debug.message_count, ws_age_s)
            except Exception:
                pass
            await asyncio.sleep(60)

    def _entry_blocked(self, market: CryptoMarket, side: str,
                       rung_idx: int) -> Optional[str]:
        if KILL_SWITCH:
            return "kill_switch"
        if market.seconds_left() < MIN_SECONDS_REMAINING:
            return f"window_too_late ({market.seconds_left()}s)"
        if (market.slug, side, rung_idx) in self.triggered:
            return "already_triggered"
        cap = MAX_CONCURRENT_POSITIONS
        if cap > 0 and self.persistence.open_count() >= cap:
            return f"max_concurrent ({cap})"
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self.persistence.daily_pnl(day) <= -abs(MAX_DAILY_LOSS_USD):
            return f"daily_loss_cap ({MAX_DAILY_LOSS_USD})"
        return None

    async def _maybe_buy(self, market: CryptoMarket, side: str, ask: float) -> None:
        if ask > ENTRY_MAX_PRICE:
            return

        # Update seen_below tracking for every rung whose threshold is above ask
        for ri, T in enumerate(ENTRY_THRESHOLDS):
            if ask < T:
                self.seen_below.add((market.slug, side, ri))

        # Check each rung in order. Fire any that have crossed.
        for ri, T in enumerate(ENTRY_THRESHOLDS):
            if ask < T:
                continue  # ask hasn't reached this rung yet
            # Require we've seen this rung's ask below T at some point
            if (market.slug, side, ri) not in self.seen_below:
                continue
            block = self._entry_blocked(market, side, ri)
            if block:
                continue

            # Lock immediately before await
            self.triggered.add((market.slug, side, ri))
            debug.event(f"TRIGGER {market.slug} {side} rung{ri+1}@{T} ask={ask:.4f}")

            try:
                pos = await self.executor.buy(
                    market=market, side=side,
                    size_usd=POSITION_SIZE_USD, ask_price=ask,
                )
            except ExecutionError as exc:
                debug.event(f"buy FAILED {market.slug} {side} rung{ri+1}: {exc} (lock kept)")
                continue
            except Exception as exc:
                log.exception("buy UNEXPECTED %s %s rung%d: %s", market.slug, side, ri, exc)
                continue

            pos_id = self.persistence.record_position_open(pos)
            debug.event(f"ENTERED #{pos_id} {side} rung{ri+1} {market.slug} @ {pos.entry_price:.4f} "
                        f"({pos.shares:.4f} shares, {market.seconds_left()}s left)")

    async def healthcheck_server(self) -> None:
        try:
            from aiohttp import web  # type: ignore
        except ImportError:
            log.info("aiohttp not installed — skipping healthcheck server")
            return

        bot = self  # closure

        def now_iso() -> str:
            return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        def status_json() -> dict:
            s = bot.persistence.stats_today()
            current = {}
            for slug, m in bot.markets.items():
                up_book = bot.books.get(m.up_token_id)
                dn_book = bot.books.get(m.down_token_id)
                # Per-rung trigger state: "✓✓" if all fired, "✓·" if only first, etc.
                up_trig = [(slug, "UP", ri) in bot.triggered
                           for ri in range(len(ENTRY_THRESHOLDS))]
                dn_trig = [(slug, "DOWN", ri) in bot.triggered
                           for ri in range(len(ENTRY_THRESHOLDS))]
                current[slug] = {
                    "coin": m.coin,
                    "seconds_left": m.seconds_left(),
                    "up_ask": up_book.best_ask() if up_book else None,
                    "up_bid": up_book.best_bid() if up_book else None,
                    "up_levels": len(up_book.asks) if up_book else 0,
                    "down_ask": dn_book.best_ask() if dn_book else None,
                    "down_bid": dn_book.best_bid() if dn_book else None,
                    "down_levels": len(dn_book.asks) if dn_book else 0,
                    # Back-compat fields (any rung fired = true)
                    "triggered_up": any(up_trig),
                    "triggered_down": any(dn_trig),
                    # New per-rung fields
                    "triggered_up_rungs": up_trig,
                    "triggered_down_rungs": dn_trig,
                }
            ws_age = ((time.time() - debug.last_ws_message_at)
                      if debug.last_ws_message_at else None)
            return {
                "status": "ok",
                "now": now_iso(),
                "uptime_seconds": int(time.time() - bot.start_time),
                "live_mode": is_live_mode(),
                "config": {
                    "strategy": STRATEGY,
                    "coins": COINS,
                    "threshold": ENTRY_THRESHOLD,           # back-compat: lowest rung
                    "thresholds": ENTRY_THRESHOLDS,          # full ladder
                    "limit_prices": LIMIT_PRICES,            # legacy: for STRATEGY=limits old style
                    "limit_buy_prices": LIMIT_BUY_PRICES,    # pre-market BUY ladder
                    "limit_sell_price": LIMIT_SELL_PRICE,    # SELL price after BUY fill
                    "cancel_buys_at_open_delay": CANCEL_BUYS_AT_OPEN_DELAY,
                    "max_price": ENTRY_MAX_PRICE,
                    "stake_usd": POSITION_SIZE_USD,
                    "min_seconds_remaining": MIN_SECONDS_REMAINING,
                    "max_concurrent": MAX_CONCURRENT_POSITIONS,
                    "max_daily_loss": MAX_DAILY_LOSS_USD,
                    "kill_switch": KILL_SWITCH,
                    "dry_run": DRY_RUN,
                    "trading_enabled": TRADING_ENABLED,
                    "armed_for_live": ARMED_FOR_LIVE,
                },
                "ws": {
                    "connect_count": debug.connect_count,
                    "message_count": debug.message_count,
                    "last_message_age_seconds": ws_age,
                    "last_book_snapshot_age_seconds": (
                        time.time() - debug.last_book_snapshot_at
                        if debug.last_book_snapshot_at else None),
                    "last_price_change_age_seconds": (
                        time.time() - debug.last_price_change_at
                        if debug.last_price_change_at else None),
                    "unknown_event_types": dict(debug.unknown_event_types),
                },
                "markets_tracked": len(bot.markets),
                "today": dict(s),
                "current": current,
            }

        async def handle_status(_req):
            return web.json_response(status_json())

        async def handle_ws_log(_req):
            return web.json_response({
                "connect_count": debug.connect_count,
                "message_count": debug.message_count,
                "messages": list(debug.ws_messages),
            })

        async def handle_trades(_req):
            return web.json_response({
                "recent_orders": bot.persistence.recent_orders(50),
                "recent_positions": bot.persistence.recent_positions(50),
            })

        async def handle_debug_html(_req):
            return web.Response(text=DEBUG_HTML, content_type="text/html")

        app = web.Application()
        app.router.add_get("/", handle_status)
        app.router.add_get("/healthz", handle_status)
        app.router.add_get("/ws-log", handle_ws_log)
        app.router.add_get("/trades", handle_trades)
        app.router.add_get("/debug", handle_debug_html)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", HEALTHCHECK_PORT)
        await site.start()
        log.info("HTTP server on :%d  →  /  /debug  /ws-log  /trades", HEALTHCHECK_PORT)

    async def run(self) -> None:
        log.info("─" * 60)
        log.info("Polymarket 5m crypto bot")
        log.info("  strategy:  %s", STRATEGY.upper())
        log.info("  coins:     %s", ",".join(COINS))
        if STRATEGY == "limits":
            log.info("  buy ladder: %s @ $%.2f (UP+DOWN, %d orders/market)",
                     LIMIT_BUY_PRICES, POSITION_SIZE_USD, 2 * len(LIMIT_BUY_PRICES))
            log.info("  sell on fill at: %.2f", LIMIT_SELL_PRICE)
            log.info("  cancel unfilled buys: %ds after window open",
                     CANCEL_BUYS_AT_OPEN_DELAY)
        else:
            log.info("  threshold: ladder %s, max %.2f", ENTRY_THRESHOLDS, ENTRY_MAX_PRICE)
            log.info("  size:      $%.2f", POSITION_SIZE_USD)
        log.info("  mode:      %s", "LIVE" if is_live_mode() else "DRY-RUN")
        log.info("─" * 60)

        try:
            await asyncio.gather(
                self.discovery_loop(),
                self.websocket_loop(),
                self.user_ws_loop(),
                self.resolution_loop(),
                self.status_loop(),
                self.healthcheck_server(),
            )
        finally:
            await self.client.close()


# ─── /debug HTML page ─────────────────────────────────────────────────────
DEBUG_HTML = r"""<!doctype html>
<html><head>
<meta charset="utf-8">
<title>poly5m bot · debug</title>
<style>
  body { background:#0a0a0a; color:#e0e0e0; font: 13px/1.4 ui-monospace, Menlo, Consolas, monospace; margin:0; padding:16px; }
  h1, h2 { font-weight:600; margin: 12px 0 6px; }
  h1 { font-size:18px; color:#f5d14a; }
  h2 { font-size:13px; color:#888; text-transform:uppercase; letter-spacing:.1em; border-top:1px solid #222; padding-top:14px; margin-top:18px; }
  .row { display:flex; gap:16px; flex-wrap:wrap; }
  .box { border:1px solid #222; padding:10px 12px; background:#111; min-width:160px; }
  .box .label { color:#888; font-size:11px; text-transform:uppercase; letter-spacing:.08em; }
  .box .val { font-size:18px; color:#fff; margin-top:4px; }
  .good { color:#4ade80; }
  .bad  { color:#ef4444; }
  .warn { color:#f5d14a; }
  .dim  { color:#666; }
  table { border-collapse:collapse; width:100%; margin-top:8px; font-size:12px; }
  th, td { text-align:left; padding:5px 10px; border-bottom:1px solid #1a1a1a; }
  th { color:#888; font-weight:500; text-transform:uppercase; font-size:11px; letter-spacing:.05em; }
  td.num { text-align:right; font-variant-numeric: tabular-nums; }
  .pill { display:inline-block; padding:1px 6px; border-radius:3px; font-size:11px; }
  .pill.live { background:#7f1d1d; color:#fecaca; }
  .pill.dry  { background:#1e3a8a; color:#bfdbfe; }
  pre { background:#0a0a0a; border:1px solid #1a1a1a; padding:8px; max-height:240px; overflow:auto; font-size:11px; color:#aaa; white-space:pre-wrap; word-break:break-all; }
  a { color:#60a5fa; }
</style>
</head><body>

<h1>poly5m bot · debug <span id="mode-pill"></span></h1>
<div class="dim" id="meta"></div>

<h2>Health</h2>
<div class="row" id="health"></div>

<h2>Today</h2>
<div class="row" id="today"></div>

<h2>Active markets</h2>
<table id="markets-tbl">
  <thead><tr>
    <th>SLUG</th><th>COIN</th><th class="num">SECS LEFT</th>
    <th class="num">UP ASK</th><th class="num">UP BID</th><th class="num">UP LV</th><th>UP TRIG</th>
    <th class="num">DN ASK</th><th class="num">DN BID</th><th class="num">DN LV</th><th>DN TRIG</th>
  </tr></thead>
  <tbody></tbody>
</table>

<h2>Recent events <span class="dim">(live)</span></h2>
<pre id="events"></pre>

<h2>Last 30 raw WS messages</h2>
<pre id="ws"></pre>

<h2>Recent orders</h2>
<table id="orders-tbl">
  <thead><tr>
    <th>TS</th><th>ACTION</th><th>COIN</th><th>SIDE</th>
    <th class="num">PRICE</th><th class="num">SIZE</th><th class="num">SHARES</th><th>STATUS</th>
  </tr></thead>
  <tbody></tbody>
</table>

<h2>Recent positions</h2>
<table id="positions-tbl">
  <thead><tr>
    <th>TS</th><th>SLUG</th><th>SIDE</th>
    <th class="num">ENTRY</th><th class="num">SHARES</th><th>STATUS</th><th class="num">P&amp;L</th>
  </tr></thead>
  <tbody></tbody>
</table>

<script>
const fmt = {
  pct: n => (n == null) ? '—' : (n*100).toFixed(1) + '%',
  num: n => (n == null) ? '—' : Number(n).toFixed(4),
  ago: s => s == null ? 'never' : (s < 60 ? Math.round(s)+'s' : Math.round(s/60)+'m') + ' ago',
  usd: n => (n == null) ? '—' : (n >= 0 ? '+$' : '-$') + Math.abs(n).toFixed(2),
};

function colorAge(ageSec) {
  if (ageSec == null) return 'bad';
  if (ageSec < 10) return 'good';
  if (ageSec < 60) return 'warn';
  return 'bad';
}

async function refresh() {
  try {
    const [statusR, wsR, tradesR] = await Promise.all([
      fetch('/').then(r => r.json()),
      fetch('/ws-log').then(r => r.json()),
      fetch('/trades').then(r => r.json()),
    ]);

    document.getElementById('mode-pill').innerHTML =
      statusR.live_mode
        ? '<span class="pill live">LIVE</span>'
        : '<span class="pill dry">DRY-RUN</span>';

    document.getElementById('meta').textContent =
      `${statusR.now}  ·  uptime ${Math.round(statusR.uptime_seconds/60)}min  ·  ` +
      `coins ${statusR.config.coins.join(',')}  ·  threshold ${statusR.config.threshold}  ·  ` +
      `stake $${statusR.config.stake_usd}  ·  max_concurrent ${statusR.config.max_concurrent}`;

    const ws = statusR.ws;
    const wsAgeClass = colorAge(ws.last_message_age_seconds);
    document.getElementById('health').innerHTML = `
      <div class="box"><div class="label">WS connects</div><div class="val">${ws.connect_count}</div></div>
      <div class="box"><div class="label">WS messages</div><div class="val">${ws.message_count}</div></div>
      <div class="box"><div class="label">Last WS msg</div><div class="val ${wsAgeClass}">${fmt.ago(ws.last_message_age_seconds)}</div></div>
      <div class="box"><div class="label">Last book snap</div><div class="val ${colorAge(ws.last_book_snapshot_age_seconds)}">${fmt.ago(ws.last_book_snapshot_age_seconds)}</div></div>
      <div class="box"><div class="label">Last price chg</div><div class="val ${colorAge(ws.last_price_change_age_seconds)}">${fmt.ago(ws.last_price_change_age_seconds)}</div></div>
      <div class="box"><div class="label">Markets tracked</div><div class="val">${statusR.markets_tracked}</div></div>
      <div class="box"><div class="label">Unknown WS events</div><div class="val ${Object.keys(ws.unknown_event_types || {}).length ? 'warn' : ''}">${JSON.stringify(ws.unknown_event_types || {})}</div></div>
    `;

    const t = statusR.today;
    const closed = (t.wins||0)+(t.losses||0);
    const wr = closed ? (t.wins/closed) : null;
    const pnl = t.pnl || 0;
    document.getElementById('today').innerHTML = `
      <div class="box"><div class="label">Open</div><div class="val">${t.open_n||0}</div></div>
      <div class="box"><div class="label">Wins</div><div class="val good">${t.wins||0}</div></div>
      <div class="box"><div class="label">Losses</div><div class="val bad">${t.losses||0}</div></div>
      <div class="box"><div class="label">Win rate</div><div class="val">${fmt.pct(wr)}</div></div>
      <div class="box"><div class="label">P&amp;L today</div><div class="val ${pnl>0?'good':pnl<0?'bad':''}">${fmt.usd(pnl)}</div></div>
    `;

    const mTbody = document.querySelector('#markets-tbl tbody');
    mTbody.innerHTML = '';
    const cur = statusR.current || {};
    const slugs = Object.keys(cur).sort();
    if (slugs.length === 0) {
      mTbody.innerHTML = '<tr><td colspan="11" class="dim">no active markets — discovery in progress</td></tr>';
    }
    for (const slug of slugs) {
      const c = cur[slug];
      const upHit  = c.up_ask  != null && c.up_ask  >= statusR.config.threshold;
      const dnHit  = c.down_ask != null && c.down_ask >= statusR.config.threshold;
      mTbody.innerHTML += `
        <tr>
          <td>${slug}</td>
          <td>${c.coin}</td>
          <td class="num ${c.seconds_left<MIN_SEC?'warn':''}">${c.seconds_left}</td>
          <td class="num ${upHit?'good':''}">${fmt.num(c.up_ask)}</td>
          <td class="num dim">${fmt.num(c.up_bid)}</td>
          <td class="num dim">${c.up_levels}</td>
          <td class="${c.triggered_up?'good':'dim'}">${c.triggered_up?'✓':'·'}</td>
          <td class="num ${dnHit?'good':''}">${fmt.num(c.down_ask)}</td>
          <td class="num dim">${fmt.num(c.down_bid)}</td>
          <td class="num dim">${c.down_levels}</td>
          <td class="${c.triggered_down?'good':'dim'}">${c.triggered_down?'✓':'·'}</td>
        </tr>`;
    }

    // WS messages
    const msgs = (wsR.messages || []).slice(-30).reverse();
    document.getElementById('ws').textContent =
      msgs.map(m => `[${new Date(m.t*1000).toLocaleTimeString()}] ${m.kind}: ${m.raw}`).join('\n') || '(none)';

    // Orders
    const oTbody = document.querySelector('#orders-tbl tbody');
    oTbody.innerHTML = '';
    for (const o of (tradesR.recent_orders || []).slice(0,20)) {
      oTbody.innerHTML += `
        <tr>
          <td class="dim">${o.ts ? o.ts.slice(11,19) : ''}</td>
          <td>${o.action}</td>
          <td>${o.coin}</td>
          <td>${o.side}</td>
          <td class="num">${fmt.num(o.price)}</td>
          <td class="num">$${(o.size_usd||0).toFixed(2)}</td>
          <td class="num">${fmt.num(o.shares)}</td>
          <td class="dim">${o.status}</td>
        </tr>`;
    }
    if (!tradesR.recent_orders || !tradesR.recent_orders.length) {
      oTbody.innerHTML = '<tr><td colspan="8" class="dim">no orders yet</td></tr>';
    }

    // Positions
    const pTbody = document.querySelector('#positions-tbl tbody');
    pTbody.innerHTML = '';
    for (const p of (tradesR.recent_positions || []).slice(0,20)) {
      const cls = p.pnl_usd > 0 ? 'good' : p.pnl_usd < 0 ? 'bad' : 'dim';
      pTbody.innerHTML += `
        <tr>
          <td class="dim">${p.ts ? p.ts.slice(11,19) : ''}</td>
          <td>${p.slug || ''}</td>
          <td>${p.side}</td>
          <td class="num">${fmt.num(p.entry_price)}</td>
          <td class="num">${fmt.num(p.shares)}</td>
          <td>${p.status}</td>
          <td class="num ${cls}">${p.pnl_usd != null ? fmt.usd(p.pnl_usd) : '—'}</td>
        </tr>`;
    }
    if (!tradesR.recent_positions || !tradesR.recent_positions.length) {
      pTbody.innerHTML = '<tr><td colspan="7" class="dim">no positions yet</td></tr>';
    }
  } catch (e) {
    document.getElementById('meta').textContent = 'fetch error: ' + e.message;
  }
}

const MIN_SEC = 20;
refresh();
setInterval(refresh, 3000);
</script>
</body></html>
"""


def main() -> None:
    if is_live_mode() and not POLY_PRIVATE_KEY:
        log.error("LIVE armed but POLY_PRIVATE_KEY empty. Aborting.")
        sys.exit(1)
    bot = Bot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        log.info("shutting down")


if __name__ == "__main__":
    main()
