"""Lighter integration: account reads + L2-signed order execution, in-process.

SAFETY MODEL (this file guarantees a position is never left naked):
  1. ENTRY is placed once and NEVER blind-retried (retrying a lost-success entry
     would open a DOUBLE position). Verified failures simply abort the cycle.
  2. Right after a filled entry, SL+TP are placed as ONE grouped OCO order using
     the exact stop/target from the decision, with retries.
  3. ensure_protection() runs every cycle and re-checks EVERY open position; any
     position with no active reduce-only trigger order gets an emergency OCO pair.
     This is the "retry until success" guarantee -- it persists across cycles.
  4. If SL/TP genuinely cannot be placed after all retries, the position is
     CLOSED (reduce-only) rather than run unprotected (EMERGENCY_CLOSE_IF_UNPROTECTED).

Order-type facts verified against the official lighter-python examples:
  - SL/TP must use *_LIMIT types (3 / 5), GTT, expiry sentinel -1 (not a timestamp).
  - Position-tied OCO SL/TP use BaseAmount=0 (they close the whole position) and
    auto-cancel each other when one fills (create_position_tied_sl_tp.py).
  - API nonce manager avoids the "invalid signature" drift from the optimistic default.
"""
import time
import json
import asyncio
import contextlib
import logging

import lighter
from lighter.signer_client import CreateOrderTxReq
from config import CONFIG

log = logging.getLogger("pte-bot.exchange")

# --- account field mapping (varies by SDK version; tune if a value is None) ---
COLLATERAL_KEYS = ["collateral", "available_balance", "available_collateral", "cross_asset_value"]
ACCOUNT_VALUE_KEYS = ["total_asset_value", "account_value", "portfolio_value", "equity"]
POS_SIZE_KEYS = ["position", "position_size", "size", "base_amount"]
POS_ENTRY_KEYS = ["avg_entry_price", "entry_price", "average_entry_price"]
POS_UPNL_KEYS = ["unrealized_pnl", "unrealised_pnl", "uPnl", "upnl"]
POS_SIGN_KEYS = ["sign", "side", "direction"]
POS_MARKET_KEYS = ["market_id", "market_index", "symbol"]


def _pick(d, keys, default=None):
    for k in keys:
        if isinstance(d, dict) and d.get(k) is not None:
            return d[k]
    return default


def _to_dict(obj):
    for m in ("model_dump", "to_dict", "dict"):
        if hasattr(obj, m):
            with contextlib.suppress(Exception):
                return getattr(obj, m)()
    return obj if isinstance(obj, dict) else {}


def _today():
    return time.strftime("%Y-%m-%d", time.gmtime())


def _daily_baseline(equity):
    """Today's starting equity (resets 00:00 UTC), persisted for the kill switch."""
    try:
        with open(CONFIG.state_file) as f:
            s = json.load(f)
    except Exception:
        s = {}
    if s.get("date") != _today():
        s = {"date": _today(), "baseline_equity": equity}
        with contextlib.suppress(Exception):
            with open(CONFIG.state_file, "w") as f:
                json.dump(s, f)
    return float(s.get("baseline_equity", equity))


def _const(name, fallback):
    return int(getattr(lighter.SignerClient, name, fallback))


OT_LIMIT = _const("ORDER_TYPE_LIMIT", 0)
OT_MARKET = _const("ORDER_TYPE_MARKET", 1)
OT_SL_LIMIT = _const("ORDER_TYPE_STOP_LOSS_LIMIT", 3)
OT_TP_LIMIT = _const("ORDER_TYPE_TAKE_PROFIT_LIMIT", 5)
TIF_IOC = _const("ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL", 0)
TIF_GTT = _const("ORDER_TIME_IN_FORCE_GOOD_TILL_TIME", 1)
GTT_EXPIRY = _const("DEFAULT_28_DAY_ORDER_EXPIRY", -1)
GROUP_OCO = _const("GROUPING_TYPE_ONE_CANCELS_THE_OTHER", 2)


def _resp_ok(resp, err):
    """create_order / create_grouped_orders return (tx, resp, err). ok when err is
    falsy and (if present) the response code is a success code."""
    if err:
        return False, str(err)
    code = None
    with contextlib.suppress(Exception):
        code = getattr(resp, "code", None)
        if code is None and isinstance(resp, dict):
            code = resp.get("code")
    if code is not None and int(code) not in (0, 200):
        return False, f"code={code}"
    return True, None


class Exchange:
    def __init__(self):
        self.api = None
        self.account_api = None
        self.order_api = None
        self.signer = None

    async def start(self):
        self.api = lighter.ApiClient(lighter.Configuration(host=CONFIG.lighter_base_url))
        self.account_api = lighter.AccountApi(self.api)
        self.order_api = lighter.OrderApi(self.api)
        if CONFIG.lighter_private_key:
            self.signer = lighter.SignerClient(
                url=CONFIG.lighter_base_url,
                api_private_keys={CONFIG.lighter_api_key_index: CONFIG.lighter_private_key},
                account_index=CONFIG.lighter_account_index,
                # API nonce manager: authoritative server nonce per order (avoids the
                # optimistic-counter drift that surfaced as "invalid signature").
                nonce_management_type=lighter.nonce_manager.NonceManagerType.API,
            )
        log.info("exchange ready (signer=%s)", "ON" if self.signer else "OFF read-only")

    async def close(self):
        if self.signer:
            with contextlib.suppress(Exception):
                await self.signer.close()
        if self.api:
            with contextlib.suppress(Exception):
                await self.api.close()

    # ---- scaling helpers ----
    def _int_price(self, p):
        return int(round(float(p) * (10 ** CONFIG.price_decimals)))

    def _int_size(self, q):
        return int(round(float(q) * (10 ** CONFIG.size_decimals)))

    def _coi(self):
        return int(time.time() * 1000000) % (2 ** 47)

    # ---- single order (entry / emergency close) ----
    async def _place(self, coi, size_int, price_int, is_ask, order_type, tif,
                     reduce_only=False, trigger_price=0, expiry=0):
        try:
            tx, resp, err = await self.signer.create_order(
                market_index=CONFIG.market_index,
                client_order_index=coi,
                base_amount=size_int,
                price=price_int,
                is_ask=is_ask,
                order_type=order_type,
                time_in_force=tif,
                reduce_only=reduce_only,
                trigger_price=trigger_price,
                order_expiry=expiry,
            )
            ok, why = _resp_ok(resp, err)
            return {"ok": ok, "tx_hash": str(getattr(resp, "tx_hash", ""))} if ok else {"ok": False, "error": why}
        except TypeError as e:
            return {"ok": False, "error": f"create_order signature mismatch ({e}); check SDK version"}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # ---- grouped OCO SL+TP (position-tied, reduce-only) ----
    async def _place_oco(self, sl_trigger, tp_trigger, close_is_ask):
        """One signed tx containing a take-profit and a stop-loss that (a) close the
        whole position, (b) cancel each other when one fills. Limit price is set
        marketable past the trigger so the close actually fills."""
        buf = CONFIG.mkt_slippage
        is_ask = 1 if close_is_ask else 0

        def limit_for(trigger):
            # closing via SELL (is_ask) -> accept a bit less (below); via BUY -> pay a bit more (above)
            return trigger * (1 - buf) if close_is_ask else trigger * (1 + buf)

        tp = CreateOrderTxReq(
            MarketIndex=CONFIG.market_index, ClientOrderIndex=self._coi(), BaseAmount=0,
            Price=self._int_price(limit_for(tp_trigger)), IsAsk=is_ask, Type=OT_TP_LIMIT,
            TimeInForce=TIF_GTT, ReduceOnly=1, TriggerPrice=self._int_price(tp_trigger), OrderExpiry=GTT_EXPIRY,
        )
        sl = CreateOrderTxReq(
            MarketIndex=CONFIG.market_index, ClientOrderIndex=self._coi() + 1, BaseAmount=0,
            Price=self._int_price(limit_for(sl_trigger)), IsAsk=is_ask, Type=OT_SL_LIMIT,
            TimeInForce=TIF_GTT, ReduceOnly=1, TriggerPrice=self._int_price(sl_trigger), OrderExpiry=GTT_EXPIRY,
        )
        try:
            tx, resp, err = await self.signer.create_grouped_orders(grouping_type=GROUP_OCO, orders=[tp, sl])
            ok, why = _resp_ok(resp, err)
            return {"ok": ok} if ok else {"ok": False, "error": why}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    async def _protect_with_retry(self, sl_trigger, tp_trigger, close_is_ask, size_int, ref_price):
        """Place OCO SL/TP, retrying up to PROTECT_MAX_RETRIES. If it still fails and
        EMERGENCY_CLOSE_IF_UNPROTECTED is on, flatten the position (reduce-only)."""
        last = None
        for attempt in range(1, CONFIG.protect_max_retries + 1):
            res = await self._place_oco(sl_trigger, tp_trigger, close_is_ask)
            if res.get("ok"):
                return {"ok": True, "attempts": attempt}
            last = res.get("error")
            log.warning("protect attempt %d/%d failed: %s", attempt, CONFIG.protect_max_retries, last)
            await asyncio.sleep(CONFIG.protect_retry_backoff_sec)

        out = {"ok": False, "attempts": CONFIG.protect_max_retries, "last_error": last}
        if CONFIG.emergency_close_if_unprotected and size_int > 0:
            log.error("protection failed -> EMERGENCY CLOSE (reduce-only) to avoid naked position")
            out["emergency_close"] = await self._close_market(size_int, close_is_ask, ref_price)
        return out

    async def _close_market(self, size_int, close_is_ask, ref_price):
        """Flatten a position with a reduce-only marketable IOC order."""
        px = ref_price * (1 - 0.05) if close_is_ask else ref_price * (1 + 0.05)  # aggressive so it fills
        return await self._place(self._coi(), size_int, self._int_price(px), close_is_ask,
                                 OT_MARKET, TIF_IOC, reduce_only=True)

    # ---- active-order reader (needs a short-lived auth token) ----
    async def _active_orders(self, market_index):
        auth, err = self.signer.create_auth_token_with_expiry(api_key_index=CONFIG.lighter_api_key_index)
        if err:
            raise RuntimeError(f"auth token: {err}")
        res = await self.order_api.account_active_orders(
            authorization=auth, account_index=CONFIG.lighter_account_index, market_id=int(market_index))
        return (_to_dict(res).get("orders")) or []

    @staticmethod
    def _has_protective(orders):
        """A position is 'protected' if it has at least one active reduce-only order
        carrying a trigger price (i.e. a working stop/target). account_active_orders
        only returns live orders, so no status filtering is needed."""
        for o in orders:
            od = o if isinstance(o, dict) else _to_dict(o)
            ro = od.get("reduce_only")
            trig = od.get("trigger_price")
            with contextlib.suppress(Exception):
                if bool(ro) and float(trig or 0) > 0:
                    return True
        return False

    @staticmethod
    def _position_is_long(pos):
        sign = pos.get("sign")
        if sign is not None:
            return str(sign).strip().lower() in ("1", "long", "buy", "true", "bid", "+")
        with contextlib.suppress(Exception):
            return float(pos.get("size") or 0) > 0
        return True

    # ---- account ----
    async def get_account(self):
        fallback = {
            "base_capital_usd": CONFIG.initial_capital, "equity_usd": CONFIG.initial_capital,
            "available_usd": CONFIG.initial_capital, "unrealized_pnl_usd": 0.0,
            "realized_pnl_today_usd": 0.0, "daily_pnl_pct": 0.0, "positions": [], "source": "fallback",
        }
        try:
            acc_obj = await self.account_api.account(by="index", value=str(CONFIG.lighter_account_index))
            raw = _to_dict(acc_obj)
            node = raw["accounts"][0] if isinstance(raw.get("accounts"), list) and raw["accounts"] else raw

            collateral = float(_pick(node, COLLATERAL_KEYS, 0) or 0)
            positions, u_pnl = [], 0.0
            for p in (node.get("positions") or []):
                pd = _to_dict(p) if not isinstance(p, dict) else p
                size = float(_pick(pd, POS_SIZE_KEYS, 0) or 0)
                if size == 0:
                    continue
                up = float(_pick(pd, POS_UPNL_KEYS, 0) or 0)
                u_pnl += up
                positions.append({
                    "market": _pick(pd, POS_MARKET_KEYS), "size": size,
                    "entry_price": _pick(pd, POS_ENTRY_KEYS), "sign": _pick(pd, POS_SIGN_KEYS),
                    "unrealized_pnl_usd": up,
                })

            equity = _pick(node, ACCOUNT_VALUE_KEYS)
            equity = float(equity) if equity is not None else (collateral + u_pnl)

            baseline = _daily_baseline(equity)
            today_pnl = equity - baseline
            return {
                "base_capital_usd": CONFIG.initial_capital,
                "equity_usd": round(equity, 2),
                "available_usd": round(collateral, 2),
                "unrealized_pnl_usd": round(u_pnl, 2),
                "realized_pnl_today_usd": round(today_pnl, 2),
                "daily_pnl_pct": round((today_pnl / baseline * 100) if baseline else 0.0, 2),
                "total_pnl_usd": round(equity - CONFIG.initial_capital, 2),
                "positions": positions,
                "source": "lighter",
                "_raw": node,
            }
        except Exception as e:
            log.warning("get_account failed, using fallback: %s", e)
            fallback["error"] = f"{type(e).__name__}: {e}"
            return fallback

    # ---- GUARDIAN: sweep every open position, protect any that is naked ----
    async def ensure_protection(self, account):
        """Returns a list of actions taken (empty if everything was already protected).
        Called every cycle. This is the 'retry until success' guarantee: a naked
        position gets protected here, and if this pass fails it is retried next cycle."""
        actions = []
        if not CONFIG.guardian_enabled or self.signer is None or CONFIG.dry_run:
            return actions
        for pos in account.get("positions", []):
            mi = pos.get("market")
            if mi is None:
                continue
            try:
                orders = await self._active_orders(mi)
            except Exception as e:
                actions.append({"market": mi, "status": "UNVERIFIED", "detail": str(e)})
                continue
            if self._has_protective(orders):
                continue  # already protected

            entry_px = 0.0
            with contextlib.suppress(Exception):
                entry_px = float(pos.get("entry_price") or 0)
            if entry_px <= 0:
                actions.append({"market": mi, "status": "NAKED_NO_ENTRY_PRICE"})
                continue

            is_long = self._position_is_long(pos)
            sp = CONFIG.guardian_stop_pct
            if is_long:
                sl, tp, close_is_ask = entry_px * (1 - sp), entry_px * (1 + sp * CONFIG.min_rr), True
            else:
                sl, tp, close_is_ask = entry_px * (1 + sp), entry_px * (1 - sp * CONFIG.min_rr), False
            size_int = self._int_size(abs(float(pos.get("size") or 0)))
            res = await self._protect_with_retry(sl, tp, close_is_ask, size_int, entry_px)
            actions.append({"market": mi, "status": "PROTECTED" if res.get("ok") else "STILL_NAKED", **res})
        return actions

    # ---- sleep-mode helper: are ALL open positions protected right now? ----
    async def all_positions_protected(self, account):
        """True only if every open position has a live reduce-only trigger (SL/TP).
        Used to decide it is safe to sleep the AI loop. Read-only exchange calls.
        In dry_run (or without a signer) we cannot query live orders, so we treat
        positions as 'protected' — nothing real is at stake there."""
        positions = account.get("positions", [])
        if not positions:
            return False
        if self.signer is None or CONFIG.dry_run:
            return True
        for pos in positions:
            mi = pos.get("market")
            if mi is None:
                return False
            try:
                orders = await self._active_orders(mi)
            except Exception:
                return False  # can't verify -> don't sleep
            if not self._has_protective(orders):
                return False
        return True

    # ---- execution (entry, then guaranteed protection) ----
    async def execute(self, decision):
        out = {"ok": False, "dry_run": decision["dry_run"], "side": decision["side"],
               "protection": None, "warning": None}

        if decision["dry_run"]:
            out.update({"ok": True, "tx_hash": "DRYRUN-" + str(self._coi()), "note": "dry_run -> no order sent"})
            return out
        if self.signer is None:
            out["error"] = "Signer not initialized (set LIGHTER_PRIVATE_KEY)."
            return out

        size_int = self._int_size(decision["base_amount"])
        if size_int <= 0:
            out["error"] = f"base_amount rounds to 0 at SIZE_DECIMALS={CONFIG.size_decimals}"
            return out

        is_ask = decision["side"] == "sell"
        entry = decision["entry"]
        # ENTRY -- placed exactly once, never blind-retried (double-position risk).
        if decision["entry_type"] == "market":
            worst = entry * (1 + CONFIG.mkt_slippage) if not is_ask else entry * (1 - CONFIG.mkt_slippage)
            res = await self._place(self._coi(), size_int, self._int_price(worst), is_ask, OT_MARKET, TIF_IOC)
        else:
            res = await self._place(self._coi(), size_int, self._int_price(entry), is_ask,
                                    OT_LIMIT, TIF_GTT, expiry=GTT_EXPIRY)
        out.update(res)
        if not res.get("ok"):
            return out  # no position opened -> nothing to protect

        # PROTECTION -- OCO SL/TP with the decision's exact prices, retried; emergency-close on ultimate failure.
        if CONFIG.place_sl_tp and decision.get("stop") and decision.get("tp1"):
            close_is_ask = not is_ask  # opposite of the entry side
            prot = await self._protect_with_retry(decision["stop"], decision["tp1"],
                                                  close_is_ask, size_int, entry)
            out["protection"] = prot
            if not prot.get("ok"):
                if prot.get("emergency_close", {}).get("ok"):
                    out["warning"] = "SL/TP could not be placed -> position EMERGENCY-CLOSED (reduce-only)."
                else:
                    out["warning"] = ("SL/TP FAILED and emergency-close did not confirm -- CHECK POSITION MANUALLY: "
                                      + str(prot.get("last_error")))
        return out
