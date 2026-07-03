"""Binance USDT-M Futures execution layer v4.

SAFETY MODEL (posisi tidak pernah dibiarkan telanjang):
  1. ENTRY dikirim sekali, tidak pernah di-retry buta (risiko posisi dobel).
  2. Entry MARKET / limit yang langsung terisi -> SL+TP dipasang segera:
     STOP_MARKET + TAKE_PROFIT_MARKET dengan closePosition=true, workingType=MARK_PRICE.
     (Binance futures tidak punya OCO native; sisa order dibersihkan saat flat.)
  3. Entry LIMIT resting -> fill-watcher poll status order tiap WATCH_POLL_SEC dan
     memasang proteksi DETIK-DETIK setelah FILLED/PARTIALLY_FILLED
     (closePosition=true otomatis mencakup tambahan fill berikutnya).
  4. Guardian tiap siklus: posisi tanpa order proteksi -> proteksi darurat.
  5. Proteksi gagal total setelah retry -> posisi DITUTUP (reduce-only), bukan dibiarkan.
  6. Saat FLAT: SEMUA open order disapu (entry basi + sisa SL/TP yatim dari trade
     sebelumnya — sisa TP lama yang menyala bisa menutup posisi baru secara salah).

Filter market (tickSize/stepSize/minQty/minNotional) diambil LIVE dari
/fapi/v1/exchangeInfo saat start dan menimpa default config (fail-closed bila
simbol tidak ditemukan). Akun WAJIB One-way mode (bukan Hedge).

Kill/profit latch per hari UTC (00:00 UTC = 07:00 WIB) — sama dengan v3.
"""
import time
import json
import math
import asyncio
import contextlib
import logging

from config import CONFIG
from binance_client import BinanceClient, BinanceError

log = logging.getLogger("pte-bot.exchange")


# ---- state (baseline harian + latch); hari = tanggal UTC ----
def _today():
    return time.strftime("%Y-%m-%d", time.gmtime())


def _load_state():
    try:
        with open(CONFIG.state_file) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(s):
    with contextlib.suppress(Exception):
        with open(CONFIG.state_file, "w") as f:
            json.dump(s, f)


def _daily_baseline(equity):
    s = _load_state()
    if s.get("date") != _today():
        s = {"date": _today(), "baseline_equity": equity}
        _save_state(s)
    return float(s.get("baseline_equity", equity))


def kill_latched():
    return bool(_load_state().get("killed_on") == _today())


def latch_kill(daily_pnl_pct):
    s = _load_state()
    s["killed_on"] = _today()
    s["killed_at_pnl_pct"] = daily_pnl_pct
    _save_state(s)


def profit_latched():
    return bool(_load_state().get("profit_on") == _today())


def latch_profit(daily_pnl_pct):
    s = _load_state()
    s["profit_on"] = _today()
    s["profit_at_pnl_pct"] = daily_pnl_pct
    _save_state(s)


class Exchange:
    def __init__(self):
        self.c = None
        self._watch_task = None
        self.tick = None
        self.step = None
        self.min_qty = None
        self.min_notional = None

    async def start(self):
        self.c = BinanceClient()
        await self.c.start()
        # Filter market live -- fail-closed kalau simbol tak ada.
        info = await self.c.get("/fapi/v1/exchangeInfo")
        sym = next((s for s in (info.get("symbols") or []) if s.get("symbol") == CONFIG.symbol), None)
        if not sym:
            raise RuntimeError(f"Simbol {CONFIG.symbol} tidak ditemukan di exchangeInfo {CONFIG.binance_base}")
        for f in sym.get("filters", []):
            ft = f.get("filterType")
            if ft == "PRICE_FILTER":
                self.tick = float(f.get("tickSize"))
            elif ft == "LOT_SIZE":
                self.step = float(f.get("stepSize"))
                self.min_qty = float(f.get("minQty"))
            elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
                self.min_notional = float(f.get("notional") or f.get("minNotional") or 0)
        if not (self.tick and self.step):
            raise RuntimeError("tickSize/stepSize tidak terbaca dari exchangeInfo -- stop (fail-closed)")
        if self.min_notional:
            CONFIG.live_min_notional = self.min_notional  # dipakai gerbang risk.py
        log.info("filters %s: tick=%s step=%s minQty=%s minNotional=%s",
                 CONFIG.symbol, self.tick, self.step, self.min_qty, self.min_notional)

        if CONFIG.binance_api_key and not CONFIG.dry_run:
            dual = await self.c.sget("/fapi/v1/positionSide/dual")
            if str(dual.get("dualSidePosition")).lower() == "true":
                raise RuntimeError("Akun dalam HEDGE mode. Ubah ke One-way: "
                                   "Futures > Preference > Position Mode > One-way, lalu restart bot.")
            with contextlib.suppress(Exception):
                await self.c.spost("/fapi/v1/leverage", symbol=CONFIG.symbol,
                                   leverage=int(CONFIG.max_leverage))

    async def close(self):
        if self._watch_task and not self._watch_task.done():
            self._watch_task.cancel()
        if self.c:
            await self.c.close()

    # ---- pembulatan sesuai filter market (qty SELALU dibulatkan ke bawah) ----
    @staticmethod
    def _decimals(step):
        s = f"{step:.10f}".rstrip("0")
        return len(s.split(".")[1]) if "." in s else 0

    def fmt_price(self, p):
        t = self.tick or 0.1
        v = math.floor(float(p) / t + 1e-9) * t
        return f"{v:.{self._decimals(t)}f}"

    def fmt_qty(self, q):
        st = self.step or 0.001
        v = math.floor(float(q) / st + 1e-9) * st
        return f"{v:.{self._decimals(st)}f}", v

    # ---- account ----
    async def get_account(self):
        fallback = {
            "base_capital_usd": CONFIG.initial_capital, "equity_usd": CONFIG.initial_capital,
            "available_usd": CONFIG.initial_capital, "unrealized_pnl_usd": 0.0,
            "realized_pnl_today_usd": 0.0, "daily_pnl_pct": 0.0, "positions": [], "source": "fallback",
        }
        try:
            acc = await self.c.sget("/fapi/v2/account")
            equity = float(acc.get("totalMarginBalance") or 0)
            avail = float(acc.get("availableBalance") or 0)
            positions, u_pnl = [], 0.0
            for p in (acc.get("positions") or []):
                if p.get("symbol") != CONFIG.symbol:
                    continue
                size = float(p.get("positionAmt") or 0)
                if size == 0:
                    continue
                up = float(p.get("unrealizedProfit") or 0)
                u_pnl += up
                positions.append({
                    "market": p.get("symbol"), "size": size,
                    "entry_price": float(p.get("entryPrice") or 0),
                    "sign": "long" if size > 0 else "short",
                    "unrealized_pnl_usd": up,
                })
            baseline = _daily_baseline(equity)
            today_pnl = equity - baseline
            return {
                "base_capital_usd": CONFIG.initial_capital,
                "equity_usd": round(equity, 2),
                "available_usd": round(avail, 2),
                "unrealized_pnl_usd": round(u_pnl, 2),
                "realized_pnl_today_usd": round(today_pnl, 2),
                "daily_pnl_pct": round((today_pnl / baseline * 100) if baseline else 0.0, 2),
                "total_pnl_usd": round(equity - CONFIG.initial_capital, 2),
                "positions": positions,
                "source": "binance",
            }
        except Exception as e:
            log.warning("get_account failed, using fallback: %s", e)
            fallback["error"] = f"{type(e).__name__}: {e}"
            return fallback

    @staticmethod
    def open_position(account):
        for p in (account or {}).get("positions", []) or []:
            with contextlib.suppress(Exception):
                if abs(float(p.get("size") or 0)) > 0:
                    return p
        return None

    @staticmethod
    def _position_is_long(pos):
        with contextlib.suppress(Exception):
            return float(pos.get("size") or 0) > 0
        return str(pos.get("sign", "")).lower() == "long"

    # ---- orders ----
    async def open_orders(self):
        return await self.c.sget("/fapi/v1/openOrders", symbol=CONFIG.symbol) or []

    async def order_status(self, order_id):
        return await self.c.sget("/fapi/v1/order", symbol=CONFIG.symbol, orderId=order_id)

    @staticmethod
    def _is_protective(o):
        typ = str(o.get("type") or o.get("origType") or "")
        cp = str(o.get("closePosition")).lower() == "true"
        ro = str(o.get("reduceOnly")).lower() == "true" or o.get("reduceOnly") is True
        return typ in ("STOP_MARKET", "TAKE_PROFIT_MARKET", "STOP", "TAKE_PROFIT") and (cp or ro)

    async def cancel_entry_orders(self):
        """Batalkan order NON-protektif (limit entry basi). Proteksi tidak disentuh."""
        results = []
        for o in await self.open_orders():
            if self._is_protective(o):
                continue
            oid = o.get("orderId")
            try:
                await self.c.sdelete("/fapi/v1/order", symbol=CONFIG.symbol, orderId=oid)
                results.append({"order_index": oid, "ok": True, "error": None})
            except Exception as e:
                results.append({"order_index": oid, "ok": False, "error": str(e)})
        return results

    async def sweep_all_orders(self):
        """Batalkan SEMUA open order simbol ini. Dipakai saat FLAT (entry basi +
        SL/TP yatim trade lama) dan saat kill switch."""
        with contextlib.suppress(Exception):
            await self.c.sdelete("/fapi/v1/allOpenOrders", symbol=CONFIG.symbol)
            return True
        return False

    async def _cancel_protective(self):
        for o in await self.open_orders():
            if self._is_protective(o):
                with contextlib.suppress(Exception):
                    await self.c.sdelete("/fapi/v1/order", symbol=CONFIG.symbol, orderId=o.get("orderId"))

    # ---- protection: SL + TP conditional, closePosition=true ----
    async def _protect(self, sl_trigger, tp_trigger, close_side):
        try:
            await self.c.spost("/fapi/v1/order", symbol=CONFIG.symbol, side=close_side,
                               type="STOP_MARKET", stopPrice=self.fmt_price(sl_trigger),
                               closePosition="true", workingType="MARK_PRICE",
                               newClientOrderId=f"zb-sl-{int(time.time()*1000)}")
            await self.c.spost("/fapi/v1/order", symbol=CONFIG.symbol, side=close_side,
                               type="TAKE_PROFIT_MARKET", stopPrice=self.fmt_price(tp_trigger),
                               closePosition="true", workingType="MARK_PRICE",
                               newClientOrderId=f"zb-tp-{int(time.time()*1000)}")
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    async def close_position_market(self):
        """Tutup posisi simbol ini sekarang (MARKET reduce-only)."""
        acc = await self.get_account()
        pos = self.open_position(acc)
        if not pos:
            return {"ok": True, "note": "no position"}
        size = abs(float(pos.get("size") or 0))
        qty_str, qty = self.fmt_qty(size)
        if qty <= 0:
            return {"ok": False, "error": "size rounds to 0"}
        side = "SELL" if self._position_is_long(pos) else "BUY"
        try:
            await self.c.spost("/fapi/v1/order", symbol=CONFIG.symbol, side=side,
                               type="MARKET", quantity=qty_str, reduceOnly="true")
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    async def _protect_with_retry(self, sl_trigger, tp_trigger, close_side):
        last = None
        for attempt in range(1, CONFIG.protect_max_retries + 1):
            with contextlib.suppress(Exception):
                await self._cancel_protective()  # cegah duplikat SL/TP antar-percobaan
            res = await self._protect(sl_trigger, tp_trigger, close_side)
            if res.get("ok"):
                return {"ok": True, "attempts": attempt}
            last = res.get("error")
            log.warning("protect attempt %d/%d failed: %s", attempt, CONFIG.protect_max_retries, last)
            await asyncio.sleep(CONFIG.protect_retry_backoff_sec)

        out = {"ok": False, "attempts": CONFIG.protect_max_retries, "last_error": last}
        if CONFIG.emergency_close_if_unprotected:
            log.error("protection failed -> EMERGENCY CLOSE (reduce-only)")
            out["emergency_close"] = await self.close_position_market()
        return out

    # ---- GUARDIAN ----
    async def ensure_protection(self, account):
        actions = []
        if not CONFIG.guardian_enabled or CONFIG.dry_run or not CONFIG.binance_api_key:
            return actions
        pos = self.open_position(account)
        if not pos:
            return actions
        try:
            orders = await self.open_orders()
        except Exception as e:
            return [{"market": CONFIG.symbol, "status": "UNVERIFIED", "detail": str(e)}]
        if any(self._is_protective(o) for o in orders):
            return actions
        entry_px = float(pos.get("entry_price") or 0)
        if entry_px <= 0:
            return [{"market": CONFIG.symbol, "status": "NAKED_NO_ENTRY_PRICE"}]
        is_long = self._position_is_long(pos)
        sp = CONFIG.guardian_stop_pct
        if is_long:
            sl, tp, side = entry_px * (1 - sp), entry_px * (1 + sp * CONFIG.min_rr), "SELL"
        else:
            sl, tp, side = entry_px * (1 + sp), entry_px * (1 - sp * CONFIG.min_rr), "BUY"
        res = await self._protect_with_retry(sl, tp, side)
        actions.append({"market": CONFIG.symbol,
                        "status": "PROTECTED" if res.get("ok") else "STILL_NAKED", **res})
        return actions

    # ---- kill switch: flatten semuanya ----
    async def close_all_positions(self, account=None):
        out = {"canceled": False, "closed": [], "flat": None}
        if CONFIG.dry_run or not CONFIG.binance_api_key:
            out["flat"] = True
            return out
        out["canceled"] = await self.sweep_all_orders()
        res = await self.close_position_market()
        out["closed"].append({"market": CONFIG.symbol, **res})
        with contextlib.suppress(Exception):
            await asyncio.sleep(2)
            fresh = await self.get_account()
            out["flat"] = self.open_position(fresh) is None
        return out

    # ---- FILL WATCHER: proteksi diikat ke saat fill untuk limit resting ----
    def start_fill_watcher(self, decision, order_id):
        if self._watch_task and not self._watch_task.done():
            self._watch_task.cancel()
        self._watch_task = asyncio.create_task(self._watch_fill(dict(decision), order_id))

    async def _watch_fill(self, decision, order_id):
        """Poll status order sampai FILLED, lalu pasang SL/TP dalam hitungan detik.
        Umur <= satu periode loop; sapu-flat + guardian tetap backstop. Watcher mati
        bila proses bot mati -- guardian menutup celah di siklus pertama setelah restart."""
        deadline = time.time() + CONFIG.loop_minutes * 60
        close_side = "BUY" if decision["side"] == "sell" else "SELL"
        log.info("fill-watcher ON (orderId=%s, poll %ss)", order_id, CONFIG.watch_poll_sec)
        try:
            while time.time() < deadline:
                await asyncio.sleep(CONFIG.watch_poll_sec)
                try:
                    o = await self.order_status(order_id)
                    st = str(o.get("status") or "")
                    if st in ("FILLED", "PARTIALLY_FILLED"):
                        prot = await self._protect_with_retry(decision["stop"], decision["tp1"], close_side)
                        await self._watcher_notify(decision, prot)
                        return
                    if st in ("CANCELED", "EXPIRED", "REJECTED"):
                        log.info("fill-watcher: entry %s -> stop", st)
                        return
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning("fill-watcher poll error: %s", e)
        except asyncio.CancelledError:
            pass

    async def _watcher_notify(self, decision, prot):
        with contextlib.suppress(Exception):
            from notify import send  # lazy import
            if prot.get("ok"):
                msg = (f"🛡️ <b>Limit terisi → SL/TP terpasang</b> (watcher · percobaan {prot.get('attempts')})\n"
                       f"• SL ${decision['stop']:,.1f} · TP ${decision['tp1']:,.1f}")
            elif (prot.get("emergency_close") or {}).get("ok"):
                msg = "🚨 <b>Limit terisi tapi SL/TP GAGAL → posisi ditutup darurat</b> (reduce-only)"
            else:
                msg = ("⚠️ <b>Limit terisi, SL/TP GAGAL, tutup darurat tak terkonfirmasi — CEK POSISI MANUAL!</b>\n"
                       f"• error: {prot.get('last_error')}")
            await send(msg)

    # ---- execution ----
    async def execute(self, decision):
        out = {"ok": False, "dry_run": decision["dry_run"], "side": decision["side"],
               "protection": None, "warning": None}

        if decision["dry_run"]:
            out.update({"ok": True, "tx_hash": f"DRYRUN-{int(time.time())}",
                        "note": "dry_run -> no order sent"})
            return out
        if not CONFIG.binance_api_key:
            out["error"] = "BINANCE_API_KEY belum di-set."
            return out

        qty_str, qty = self.fmt_qty(decision["base_amount"])
        if qty <= 0 or (self.min_qty and qty < self.min_qty):
            out["error"] = f"qty {qty_str} < minQty {self.min_qty} (modal terlalu kecil untuk aturan risk)"
            return out
        entry = decision["entry"]
        mn = self.min_notional or CONFIG.binance_min_notional
        if mn and qty * entry < mn:
            out["error"] = f"notional ${qty * entry:,.2f} < minNotional ${mn:,.0f} Binance"
            return out

        side = "BUY" if decision["side"] == "buy" else "SELL"
        close_side = "SELL" if side == "BUY" else "BUY"
        want_protection = bool(CONFIG.place_sl_tp and decision.get("stop") and decision.get("tp1"))

        async def _arm():
            prot = await self._protect_with_retry(decision["stop"], decision["tp1"], close_side)
            out["protection"] = prot
            if not prot.get("ok"):
                if (prot.get("emergency_close") or {}).get("ok"):
                    out["warning"] = "SL/TP could not be placed -> position EMERGENCY-CLOSED (reduce-only)."
                else:
                    out["warning"] = ("SL/TP FAILED and emergency-close did not confirm -- "
                                      "CHECK POSITION MANUALLY: " + str(prot.get("last_error")))

        try:
            if decision["entry_type"] == "market":
                r = await self.c.spost("/fapi/v1/order", symbol=CONFIG.symbol, side=side,
                                       type="MARKET", quantity=qty_str)
                out.update({"ok": True, "tx_hash": str(r.get("orderId"))})
                out["entry_status"] = "filled"
                if want_protection:
                    await _arm()
                return out

            r = await self.c.spost("/fapi/v1/order", symbol=CONFIG.symbol, side=side,
                                   type="LIMIT", timeInForce="GTC",
                                   price=self.fmt_price(entry), quantity=qty_str)
            oid = r.get("orderId")
            out.update({"ok": True, "tx_hash": str(oid)})
        except BinanceError as e:
            out["error"] = str(e)
            return out
        except Exception as e:
            out["error"] = f"{type(e).__name__}: {e}"
            return out

        out["entry_status"] = "unknown"
        with contextlib.suppress(Exception):
            await asyncio.sleep(2)
            o = await self.order_status(oid)
            st = str(o.get("status") or "")
            out["entry_status"] = ("filled" if st == "FILLED"
                                   else "partial" if st == "PARTIALLY_FILLED"
                                   else "resting" if st == "NEW" else st.lower() or "unknown")

        if out["entry_status"] in ("filled", "partial"):
            # closePosition=true mencakup seluruh posisi, termasuk sisa fill berikutnya.
            if want_protection:
                await _arm()
        elif want_protection:
            if CONFIG.limit_fill_watcher:
                self.start_fill_watcher(decision, oid)
                out["protection"] = {"deferred": True, "mode": "watcher", "poll_sec": CONFIG.watch_poll_sec}
            else:
                out["protection"] = {"deferred": True, "mode": "guardian-only"}
                out["warning"] = ("Limit resting tanpa watcher: proteksi baru dipasang guardian "
                                  "pada siklus berikutnya setelah fill.")
        return out
