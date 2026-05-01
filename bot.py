"""
Polymarket 5m crypto ask-trigger bot.

Strategy: for every active {coin}-updown-5m-{ts} market, watch both the UP
and DOWN token. When either token's best ASK is at or above ENTRY_THRESHOLD
(and at or below ENTRY_MAX_PRICE), submit a market BUY for $POSITION_SIZE_USD.
Hold to resolution.

Safety triple — all three required for live trading:
    DRY_RUN=false  AND  TRADING_ENABLED=true  AND  ARMED_FOR_LIVE=true
Default is paper. Flip ARMED_FOR_LIVE last.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

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


COINS = [c.strip().lower() for c in _env("COINS", "btc,eth,xrp,doge,hype,bnb,sol").split(",") if c.strip()]
ENTRY_THRESHOLD = _env("ENTRY_THRESHOLD", 0.58, float)
ENTRY_MAX_PRICE = _env("ENTRY_MAX_PRICE", 0.97, float)
POSITION_SIZE_USD = _env("POSITION_SIZE_USD", 1.0, float)

DISCOVERY_REFRESH_SECONDS = _env("DISCOVERY_REFRESH_SECONDS", 10, int)
PRICE_POLL_INTERVAL_SECONDS = _env("PRICE_POLL_INTERVAL_SECONDS", 2, int)

MAX_CONCURRENT_POSITIONS = _env("MAX_CONCURRENT_POSITIONS", 0, int)  # 0 = unlimited
MAX_DAILY_LOSS_USD = _env("MAX_DAILY_LOSS_USD", 50.0, float)
KILL_SWITCH = _env("KILL_SWITCH", False, bool)

# Don't enter when window has less than this many seconds left.
# 5m markets resolve fast and on-chain fill takes ~3-12s.
MIN_SECONDS_REMAINING = _env("MIN_SECONDS_REMAINING", 20, int)

DRY_RUN = _env("DRY_RUN", True, bool)
TRADING_ENABLED = _env("TRADING_ENABLED", False, bool)
ARMED_FOR_LIVE = _env("ARMED_FOR_LIVE", False, bool)

GAMMA_BASE_URL = _env("GAMMA_BASE_URL", "https://gamma-api.polymarket.com")
CLOB_BASE_URL = _env("CLOB_BASE_URL", "https://clob.polymarket.com")
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


# ─── poly client ──────────────────────────────────────────────────────────
class PolyClient:
    def __init__(self) -> None:
        self.http = httpx.AsyncClient(
            timeout=10.0,
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
            headers={"user-agent": "5m-crypto-bot/1.0"},
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

    async def get_asks_bulk(self, token_ids: list[str]) -> dict[str, float]:
        """Best ASK for each token. side=BUY on /price returns the ask
        (what someone buying YES would pay). Bulk endpoint with single-call
        fallback per token on error."""
        out: dict[str, float] = {}
        if not token_ids:
            return out
        url = f"{CLOB_BASE_URL}/prices"
        body = [{"token_id": tid, "side": "BUY"} for tid in token_ids]
        try:
            r = await self.http.post(url, json=body)
            if r.status_code == 200:
                self._merge_bulk(r.json(), out)
                if out:
                    return out
        except Exception as exc:
            log.warning("bulk /prices ask error (%s), falling back", exc)

        async def one(tid: str):
            try:
                rr = await self.http.get(f"{CLOB_BASE_URL}/price",
                                         params={"token_id": tid, "side": "BUY"})
                if rr.status_code == 200:
                    d = rr.json()
                    if isinstance(d, dict) and d.get("price") is not None:
                        return tid, float(d["price"])
            except Exception:
                pass
            return tid, None

        results = await asyncio.gather(*(one(t) for t in token_ids))
        for tid, val in results:
            if val is not None:
                out[tid] = val
        return out

    @staticmethod
    def _merge_bulk(data: Any, out: dict[str, float]) -> None:
        if isinstance(data, dict):
            for tid, val in data.items():
                try:
                    out[str(tid)] = float(val)
                except (TypeError, ValueError):
                    pass
        elif isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                tid = str(item.get("token_id") or item.get("asset_id") or "")
                raw = item.get("price") if "price" in item else item.get("ask")
                if tid and raw is not None:
                    try:
                        out[tid] = float(raw)
                    except (TypeError, ValueError):
                        pass


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
            payload={"window_ts": market.window_ts, "ask_at_entry": ask_price},
        )
        return pos


class LivePolymarketExecutor(BaseExecutor):
    """V2 SDK live executor. Mirrors patterns from the weather bot:
    re-derive creds on invalid signature, ask-based price cap, balance-delta
    fill confirmation, neg_risk lookup cached per token.
    """

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

        # Live ask re-query — we use the freshest possible
        live_ask: Optional[float] = None
        try:
            ar = self.client.get_price(token_id=token_id, side="BUY")
            ax = (ar.get("price") or ar.get("ask")) if isinstance(ar, dict) else ar
            if ax is not None:
                live_ask = float(ax)
        except Exception as exc:
            log.warning("ask re-query failed (%s) — using passed ask %.4f", exc, ask_price)

        ref_ask = live_ask if live_ask is not None else ask_price

        def build_args():
            order_tick = _tick_for_price(ref_ask, market_min_tick=market.min_tick_size)
            tick_step = float(order_tick)
            # Cap = ask + 10 ticks slippage (same pattern as weather bot)
            desired = ref_ask + (tick_step * 10)
            max_allowed = round(1.0 - tick_step, _TICK_DECIMALS.get(order_tick, 2))
            order_price = _round_to_tick(min(desired, max_allowed), order_tick)
            actual_neg_risk = self._resolve_neg_risk(token_id, fallback=bool(market.neg_risk))
            return order_tick, order_price, MarketOrderArgs(
                token_id=token_id, amount=float(size_usd), side=Side.BUY,
                order_type=OrderType.FAK, price=order_price,
            ), PartialCreateOrderOptions(tick_size=order_tick, neg_risk=actual_neg_risk)

        # Pre-order balance for fill verification
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

        # 2 attempts; re-derive on invalid signature
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

        # Fill verification
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
            payload={"order_price": order_price, "ref_ask": ref_ask},
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

    async def discovery_loop(self) -> None:
        while True:
            try:
                window = current_window_ts()
                expired = [s for s, m in self.markets.items() if m.window_ts < window]
                for s in expired:
                    self.markets.pop(s, None)
                missing = [(c, window) for c in COINS
                           if f"{c}-updown-5m-{window}" not in self.markets]
                if missing:
                    results = await asyncio.gather(
                        *(fetch_market(self.client, c, w) for c, w in missing)
                    )
                    for m in results:
                        if m is not None and m.slug not in self.markets:
                            log.info("discovered %s (%ds left)", m.slug, m.seconds_left())
                            self.markets[m.slug] = m
            except Exception as exc:
                log.exception("discovery error: %s", exc)
            await asyncio.sleep(DISCOVERY_REFRESH_SECONDS)

    async def price_loop(self) -> None:
        while True:
            try:
                if not self.markets:
                    await asyncio.sleep(PRICE_POLL_INTERVAL_SECONDS)
                    continue
                # Fetch ASK for both UP and DOWN tokens for every market
                token_ids: list[str] = []
                for m in self.markets.values():
                    token_ids.append(m.up_token_id)
                    token_ids.append(m.down_token_id)
                asks = await self.client.get_asks_bulk(token_ids)
                for slug, m in list(self.markets.items()):
                    up_ask = asks.get(m.up_token_id)
                    dn_ask = asks.get(m.down_token_id)
                    if up_ask is not None:
                        await self._maybe_buy(m, "UP", up_ask)
                    if dn_ask is not None:
                        await self._maybe_buy(m, "DOWN", dn_ask)
            except Exception as exc:
                log.exception("price loop error: %s", exc)
            await asyncio.sleep(PRICE_POLL_INTERVAL_SECONDS)

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
                log.info("📊 %d open · %dW %dL %.1f%% · pnl %+.2f · markets %d",
                         s.get("open_n") or 0, wins, losses, wr,
                         s.get("pnl") or 0.0, len(self.markets))
            except Exception:
                pass
            await asyncio.sleep(60)

    def _entry_blocked(self, market: CryptoMarket, side: str) -> Optional[str]:
        if KILL_SWITCH:
            return "kill_switch"
        if market.seconds_left() < MIN_SECONDS_REMAINING:
            return f"window_too_late ({market.seconds_left()}s)"
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
            return  # quiet — these fire constantly otherwise
        try:
            pos = await self.executor.buy(
                market=market, side=side,
                size_usd=POSITION_SIZE_USD, ask_price=ask,
            )
        except ExecutionError as exc:
            log.error("buy failed %s %s: %s", market.slug, side, exc)
            return
        except Exception as exc:
            log.exception("buy UNEXPECTED %s %s: %s", market.slug, side, exc)
            return
        pos_id = self.persistence.record_position_open(pos)
        log.info("ENTERED #%d %s %s @ %.4f ($%.2f, %.4f shares, %ds left)",
                 pos_id, side, market.slug, pos.entry_price,
                 pos.size_usd, pos.shares, market.seconds_left())

    async def healthcheck_server(self) -> None:
        """Minimal HTTP /healthz so fly.io doesn't restart the machine."""
        try:
            from aiohttp import web  # type: ignore
        except ImportError:
            log.info("aiohttp not installed — skipping healthcheck server")
            return

        async def handle(_req):
            s = self.persistence.stats_today()
            return web.json_response({
                "status": "ok",
                "live_mode": is_live_mode(),
                "markets_tracked": len(self.markets),
                "today": dict(s),
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
        log.info("Polymarket 5m crypto ask-trigger bot")
        log.info("  coins:     %s", ",".join(COINS))
        log.info("  threshold: ask >= %.2f and <= %.2f", ENTRY_THRESHOLD, ENTRY_MAX_PRICE)
        log.info("  size:      $%.2f", POSITION_SIZE_USD)
        log.info("  max conc:  %s", "unlimited" if MAX_CONCURRENT_POSITIONS <= 0 else MAX_CONCURRENT_POSITIONS)
        log.info("  daily cap: $%.2f", MAX_DAILY_LOSS_USD)
        log.info("  mode:      %s", "LIVE" if is_live_mode() else "DRY-RUN")
        log.info("─" * 60)

        try:
            await asyncio.gather(
                self.discovery_loop(),
                self.price_loop(),
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
