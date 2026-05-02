"""
Polymarket 5m crypto ask-trigger bot.

Tracks the best ASK on every UP and DOWN token of the active 5m crypto
markets via WEBSOCKET (book channel). Maintains a local order book per
token. When the best ask first reaches >= ENTRY_THRESHOLD, fires a $1
market BUY. Each (slug, side) is bought at most once per window.

Coins controlled by COINS env var (e.g. COINS=btc). Defaults to btc only.

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


# Default to btc-only. Set COINS secret to "btc,eth,sol" etc to expand.
COINS = [c.strip().lower() for c in _env("COINS", "btc").split(",") if c.strip()]
ENTRY_THRESHOLD = _env("ENTRY_THRESHOLD", 0.65, float)
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
    """Local representation of one side of one token's order book.
    For our purposes we only need the best ASK, but we keep the full
    asks dict so deltas can update it correctly."""
    asks: dict[float, float] = field(default_factory=dict)  # price -> size
    last_update_ts: float = 0.0

    def best_ask(self) -> Optional[float]:
        if not self.asks:
            return None
        return min(self.asks.keys())

    def replace_asks(self, levels: list[tuple[float, float]]) -> None:
        self.asks = {p: s for p, s in levels if s > 0}

    def apply_changes(self, changes: list[tuple[float, float]]) -> None:
        for price, size in changes:
            if size <= 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = size


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


# ─── poly REST (only for discovery + resolution) ──────────────────────────
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


# ─── execution ────────────────────────────────────────────────────────────
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
        return await loop.run_in_executor(
            None, self._buy_sync, market, side, token_id, size_usd, ask_price,
        )

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

        # The websocket-derived ask is freshest. Skip the REST re-query.
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

        self.markets: dict[str, CryptoMarket] = {}     # slug -> market
        self.books: dict[str, TokenBook] = {}          # token_id -> book
        # token_id -> (market, side) so we can look up on book updates
        self.token_lookup: dict[str, tuple[CryptoMarket, str]] = {}
        # Track which (slug,side) we've already triggered this window
        self.triggered: set[tuple[str, str]] = set()
        # Signal the websocket task that the subscription set has changed
        self.subscription_changed = asyncio.Event()

    # ─── discovery ──────────────────────────────────────────────────────
    async def discovery_loop(self) -> None:
        while True:
            try:
                window = current_window_ts()
                # Drop expired markets and their book entries
                expired = [s for s, m in self.markets.items() if m.window_ts < window]
                for s in expired:
                    m = self.markets.pop(s, None)
                    if m:
                        self.books.pop(m.up_token_id, None)
                        self.books.pop(m.down_token_id, None)
                        self.token_lookup.pop(m.up_token_id, None)
                        self.token_lookup.pop(m.down_token_id, None)
                        self.triggered.discard((m.slug, "UP"))
                        self.triggered.discard((m.slug, "DOWN"))
                if expired:
                    log.info("expired %d windows, subscriptions will refresh", len(expired))

                # Add new markets
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
                            log.info("discovered %s (%ds left)", m.slug, m.seconds_left())
                            added += 1

                if expired or added:
                    self.subscription_changed.set()
            except Exception as exc:
                log.exception("discovery error: %s", exc)
            await asyncio.sleep(DISCOVERY_REFRESH_SECONDS)

    # ─── websocket ──────────────────────────────────────────────────────
    async def websocket_loop(self) -> None:
        """Maintain a websocket subscription to all tracked tokens.
        Reconnects on disconnect, resubscribes when discovery changes the set.
        """
        backoff = 1.0
        while True:
            try:
                # Wait until we have at least one market discovered
                while not self.token_lookup:
                    await asyncio.sleep(1)

                token_ids = list(self.token_lookup.keys())
                log.info("ws connecting (%d tokens)", len(token_ids))

                async with websockets.connect(
                    CLOB_WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    backoff = 1.0  # reset on successful connect
                    sub_msg = {"assets_ids": token_ids, "type": "market"}
                    await ws.send(json.dumps(sub_msg))
                    log.info("ws subscribed to %d tokens", len(token_ids))

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
                        log.info("ws subscription set changed — reconnecting")
                    else:
                        # recv_task finished → connection died
                        exc = recv_task.exception()
                        if exc:
                            log.warning("ws recv ended: %s", exc)
                        else:
                            log.info("ws recv ended cleanly")
            except Exception as exc:
                log.warning("ws loop error: %s — backoff %.1fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _ws_recv(self, ws) -> None:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            # Polymarket sends single objects or lists of them
            messages = msg if isinstance(msg, list) else [msg]
            for m in messages:
                if not isinstance(m, dict):
                    continue
                event_type = m.get("event_type") or m.get("type")
                if event_type == "book":
                    self._handle_book_snapshot(m)
                elif event_type == "price_change":
                    self._handle_price_change(m)
                # ignore other event types (last_trade_price, tick_size_change)

            # After processing, evaluate triggers for any updated tokens
            await self._check_all_triggers()

    def _handle_book_snapshot(self, msg: dict) -> None:
        token_id = str(msg.get("asset_id") or msg.get("market") or "")
        if not token_id or token_id not in self.books:
            return
        asks_raw = msg.get("asks") or []
        levels = []
        for lvl in asks_raw:
            try:
                price = float(lvl.get("price"))
                size = float(lvl.get("size"))
                levels.append((price, size))
            except (TypeError, ValueError):
                continue
        book = self.books[token_id]
        book.replace_asks(levels)
        book.last_update_ts = time.time()

    def _handle_price_change(self, msg: dict) -> None:
        token_id = str(msg.get("asset_id") or msg.get("market") or "")
        if not token_id or token_id not in self.books:
            return
        changes_raw = msg.get("changes") or msg.get("price_changes") or []
        # Only process SELL-side changes (asks). BUY-side = bids, irrelevant.
        sell_changes = []
        for ch in changes_raw:
            side_str = str(ch.get("side", "")).upper()
            if side_str not in ("SELL", "ASK"):
                continue
            try:
                price = float(ch.get("price"))
                size = float(ch.get("size"))
                sell_changes.append((price, size))
            except (TypeError, ValueError):
                continue
        if sell_changes:
            book = self.books[token_id]
            book.apply_changes(sell_changes)
            book.last_update_ts = time.time()

    async def _check_all_triggers(self) -> None:
        for token_id, book in self.books.items():
            if token_id not in self.token_lookup:
                continue
            ask = book.best_ask()
            if ask is None:
                continue
            market, side = self.token_lookup[token_id]
            await self._maybe_buy(market, side, ask)

    # ─── resolution ─────────────────────────────────────────────────────
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
                        log.info("%s %s %s: %+.4f (entry %.4f × %.4f shares)",
                                 "✓" if won else "✗", p["side"], p["slug"],
                                 pnl, p["entry_price"], p["shares"])
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
                # Show a sample ask so we know the WS is alive
                sample = ""
                for tid, book in list(self.books.items())[:1]:
                    a = book.best_ask()
                    if a is not None and tid in self.token_lookup:
                        m, side = self.token_lookup[tid]
                        age = time.time() - book.last_update_ts
                        sample = f" · {m.coin} {side} ask {a:.3f} ({age:.0f}s ago)"
                log.info("📊 %d open · %dW %dL %.1f%% · pnl %+.2f · markets %d%s",
                         s.get("open_n") or 0, wins, losses, wr,
                         s.get("pnl") or 0.0, len(self.markets), sample)
            except Exception:
                pass
            await asyncio.sleep(60)

    def _entry_blocked(self, market: CryptoMarket, side: str) -> Optional[str]:
        if KILL_SWITCH:
            return "kill_switch"
        if market.seconds_left() < MIN_SECONDS_REMAINING:
            return f"window_too_late ({market.seconds_left()}s)"
        if (market.slug, side) in self.triggered:
            return "already_triggered"
        cap = MAX_CONCURRENT_POSITIONS
        if cap > 0 and self.persistence.open_count() >= cap:
            return f"max_concurrent ({cap})"
        if self.persistence.has_open_for_slug_side(market.slug, side):
            return "duplicate"
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self.persistence.daily_pnl(day) <= -abs(MAX_DAILY_LOSS_USD):
            return f"daily_loss_cap ({MAX_DAILY_LOSS_USD})"
        return None

    async def _maybe_buy(self, market: CryptoMarket, side: str, ask: float) -> None:
        if ask < ENTRY_THRESHOLD:
            return
        if ask > ENTRY_MAX_PRICE:
            return
        block = self._entry_blocked(market, side)
        if block:
            return

        # Mark triggered IMMEDIATELY to prevent re-entry from rapid book updates
        self.triggered.add((market.slug, side))

        try:
            pos = await self.executor.buy(
                market=market, side=side,
                size_usd=POSITION_SIZE_USD, ask_price=ask,
            )
        except ExecutionError as exc:
            log.error("buy failed %s %s: %s", market.slug, side, exc)
            # Allow retry on next book update — failed buys shouldn't lock the slot forever.
            # But only retry if window still has time; otherwise leave triggered (window's gone).
            if market.seconds_left() > MIN_SECONDS_REMAINING:
                self.triggered.discard((market.slug, side))
            return
        except Exception as exc:
            log.exception("buy UNEXPECTED %s %s: %s", market.slug, side, exc)
            if market.seconds_left() > MIN_SECONDS_REMAINING:
                self.triggered.discard((market.slug, side))
            return

        pos_id = self.persistence.record_position_open(pos)
        log.info("ENTERED #%d %s %s @ %.4f ($%.2f, %.4f shares, %ds left, ws-ask %.4f)",
                 pos_id, side, market.slug, pos.entry_price,
                 pos.size_usd, pos.shares, market.seconds_left(), ask)

    async def healthcheck_server(self) -> None:
        try:
            from aiohttp import web  # type: ignore
        except ImportError:
            log.info("aiohttp not installed — skipping healthcheck server")
            return

        async def handle(_req):
            s = self.persistence.stats_today()
            # Show fresh asks per market for debugging
            current = {}
            for slug, m in self.markets.items():
                up_book = self.books.get(m.up_token_id)
                dn_book = self.books.get(m.down_token_id)
                current[slug] = {
                    "up_ask": up_book.best_ask() if up_book else None,
                    "down_ask": dn_book.best_ask() if dn_book else None,
                    "seconds_left": m.seconds_left(),
                    "triggered_up": (slug, "UP") in self.triggered,
                    "triggered_down": (slug, "DOWN") in self.triggered,
                }
            return web.json_response({
                "status": "ok",
                "live_mode": is_live_mode(),
                "coins": COINS,
                "threshold": ENTRY_THRESHOLD,
                "stake_usd": POSITION_SIZE_USD,
                "markets_tracked": len(self.markets),
                "today": dict(s),
                "current": current,
            })

        app = web.Application()
        app.router.add_get("/healthz", handle)
        app.router.add_get("/", handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", HEALTHCHECK_PORT)
        await site.start()
        log.info("healthcheck listening on :%d", HEALTHCHECK_PORT)

    async def run(self) -> None:
        log.info("─" * 60)
        log.info("Polymarket 5m crypto WS-ask bot")
        log.info("  coins:     %s", ",".join(COINS))
        log.info("  threshold: ask >= %.2f and <= %.2f", ENTRY_THRESHOLD, ENTRY_MAX_PRICE)
        log.info("  size:      $%.2f", POSITION_SIZE_USD)
        log.info("  max conc:  %s", "unlimited" if MAX_CONCURRENT_POSITIONS <= 0 else MAX_CONCURRENT_POSITIONS)
        log.info("  daily cap: $%.2f", MAX_DAILY_LOSS_USD)
        log.info("  mode:      %s", "LIVE" if is_live_mode() else "DRY-RUN")
        log.info("  data:      websocket (%s)", CLOB_WS_URL)
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
