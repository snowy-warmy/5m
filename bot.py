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
            headers={"user-agent": "5m-crypto-bot/2.0"},
        )

    async def close(self) -> None:
        await self.http.aclose()

    async def get_event_by_slug(self, slug: str) -> Optional[dict]:
        try:
            r = await self.http.get(f"{GAMMA_BASE_URL}/events", params={"slug": slug})
            if r.status_code != 200:
                return None
            data = r.json()
            return data[0] if isinstance(data, list) and data else None
        except Exception:
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

    async def discovery_loop(self) -> None:
        while True:
            try:
                window = current_window_ts()
                expired = [s for s, m in self.markets.items() if m.window_ts < window]
                for s in expired:
                    m = self.markets.pop(s, None)
                    if m:
                        self.books.pop(m.up_token_id, None)
                        self.books.pop(m.down_token_id, None)
                        self.token_lookup.pop(m.up_token_id, None)
                        self.token_lookup.pop(m.down_token_id, None)
                        # Clear all rung-level state for this slug
                        for side in ("UP", "DOWN"):
                            for ri in range(len(ENTRY_THRESHOLDS)):
                                self.triggered.discard((m.slug, side, ri))
                                self.seen_below.discard((m.slug, side, ri))
                if expired:
                    debug.event(f"expired {len(expired)} window(s)")

                missing = [(c, window) for c in COINS
                           if f"{c}-updown-5m-{window}" not in self.markets]
                added = 0
                if missing:
                    results = await asyncio.gather(
                        *(fetch_market(self.client, c, w) for c, w in missing)
                    )
                    for m in results:
                        if m is not None and m.slug not in self.markets:
                            self.markets[m.slug] = m
                            self.books[m.up_token_id] = TokenBook()
                            self.books[m.down_token_id] = TokenBook()
                            self.token_lookup[m.up_token_id] = (m, "UP")
                            self.token_lookup[m.down_token_id] = (m, "DOWN")
                            debug.event(f"discovered {m.slug} ({m.seconds_left()}s left, "
                                        f"up={m.up_token_id[-8:]}, dn={m.down_token_id[-8:]})")
                            added += 1

                if expired or added:
                    self.subscription_changed.set()
            except Exception as exc:
                log.exception("discovery error: %s", exc)
            await asyncio.sleep(DISCOVERY_REFRESH_SECONDS)

    async def websocket_loop(self) -> None:
        backoff = 1.0
        while True:
            try:
                while not self.token_lookup:
                    await asyncio.sleep(1)

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
                    # Polymarket CLOB market WS subscribe shape
                    sub_msg = {"assets_ids": token_ids, "type": "market"}
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
                elif event_type in ("last_trade_price", "tick_size_change", ""):
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
        for token_id, book in self.books.items():
            if token_id not in self.token_lookup:
                continue
            ask = book.best_ask()
            if ask is None:
                continue
            market, side = self.token_lookup[token_id]
            await self._maybe_buy(market, side, ask)

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
                    "coins": COINS,
                    "threshold": ENTRY_THRESHOLD,           # back-compat: lowest rung
                    "thresholds": ENTRY_THRESHOLDS,          # full ladder
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
        log.info("Polymarket 5m crypto WS-ask bot (with /debug)")
        log.info("  coins:     %s", ",".join(COINS))
        log.info("  threshold: ladder %s, max %.2f", ENTRY_THRESHOLDS, ENTRY_MAX_PRICE)
        log.info("  size:      $%.2f", POSITION_SIZE_USD)
        log.info("  mode:      %s", "LIVE" if is_live_mode() else "DRY-RUN")
        log.info("─" * 60)

        try:
            await asyncio.gather(
                self.discovery_loop(),
                self.websocket_loop(),
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
