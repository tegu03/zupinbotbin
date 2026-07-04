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

    # ================= PROTECTION SUBSYSTEM (v4.2 -- reliable SL/TP) =================
    # Alur: WAIT POSITION READY -> cek breach vs MARK -> PLACE SL -> VERIFY -> PLACE TP
    # -> VERIFY. Retry berbasis state (refresh mark tiap percobaan). Kalau trigger sudah
    # dilewati mark (mis. error -2021 "would immediately trigger") -> MARKET close, bukan
    # mengirim order mustahil. Kalau SL tak bisa terpasang -> EMERGENCY close (tak telanjang).

    async def mark_price(self):
        with contextlib.suppress(Exception):
            r = await self.c.get("/fapi/v1/premiumIndex", symbol=CONFIG.symbol)
            m = float(r.get("markPrice") or 0)
            return m or None
        return None

    async def _position_risk(self):
        """(positionAmt, entryPrice, markPrice) langsung dari /fapi/v2/positionRisk."""
        data = await self.c.sget("/fapi/v2/positionRisk", symbol=CONFIG.symbol)
        row = None
        if isinstance(data, list):
            row = next((r for r in data if r.get("symbol") == CONFIG.symbol), None)
        elif isinstance(data, dict):
            row = data
        if not row:
            return 0.0, 0.0, None
        amt = float(row.get("positionAmt") or 0)
        entry = float(row.get("entryPrice") or 0)
        mark = float(row.get("markPrice") or 0) or None
        return amt, entry, mark

    async def _wait_position(self):
        """Tunggu posisi BENAR-BENAR aktif (positionAmt != 0 & entryPrice > 0) sebelum
        memasang proteksi. Menutup race condition: FILLED != posisi langsung queryable."""
        deadline = time.time() + CONFIG.position_wait_timeout_sec
        amt = entry = 0.0
        mark = None
        while time.time() < deadline:
            with contextlib.suppress(Exception):
                amt, entry, mark = await self._position_risk()
            if amt != 0 and entry > 0:
                return amt, entry, (mark or await self.mark_price())
            await asyncio.sleep(CONFIG.position_wait_interval_sec)
        return amt, entry, (mark or await self.mark_price())

    @staticmethod
    def _order_kind(o):
        typ = str(o.get("type") or o.get("origType") or "")
        if typ in ("STOP_MARKET", "STOP"):
            return "sl"
        if typ in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT"):
            return "tp"
        return None

    async def _open_protective_map(self):
        """{'sl': order|None, 'tp': order|None} dari open orders yang protektif."""
        out = {"sl": None, "tp": None}
        for o in await self.open_orders():
            if not self._is_protective(o):
                continue
            k = self._order_kind(o)
            if k and out.get(k) is None:
                out[k] = o
        return out

    @staticmethod
    def _breached(kind, is_long, mark, trigger):
        """True jika mark sudah melewati sisi pemicu (order akan langsung trigger = -2021)."""
        if mark is None:
            return False
        if kind == "sl":
            return mark <= trigger if is_long else mark >= trigger
        return mark >= trigger if is_long else mark <= trigger

    async def _place_one(self, kind, side, trigger):
        otype = "STOP_MARKET" if kind == "sl" else "TAKE_PROFIT_MARKET"
        t0 = time.time()
        try:
            r = await self.c.spost("/fapi/v1/order", symbol=CONFIG.symbol, side=side,
                                   type=otype, stopPrice=self.fmt_price(trigger),
                                   closePosition="true", workingType="MARK_PRICE",
                                   newClientOrderId=f"zb-{kind}-{int(time.time()*1000)}")
            return {"ok": True, "orderId": r.get("orderId"),
                    "latency_ms": int((time.time() - t0) * 1000)}
        except BinanceError as e:
            return {"ok": False, "code": e.code, "error": e.msg,
                    "latency_ms": int((time.time() - t0) * 1000)}
        except Exception as e:
            return {"ok": False, "code": None, "error": f"{type(e).__name__}: {e}",
                    "latency_ms": int((time.time() - t0) * 1000)}

    async def _verify_leg(self, kind):
        """Pastikan leg benar-benar ADA di open orders (bukan sekadar submit sukses)."""
        deadline = time.time() + CONFIG.verify_timeout_sec
        while time.time() < deadline:
            with contextlib.suppress(Exception):
                if (await self._open_protective_map()).get(kind):
                    return True
            await asyncio.sleep(CONFIG.verify_interval_sec)
        return False

    async def _ensure_leg(self, kind, side, trigger, is_long):
        """Pasang SATU leg (sl/tp) sampai TERVERIFIKASI. State-based retry (refresh mark).
        Return breached=True jika mark sudah melewati trigger (caller -> MARKET close)."""
        with contextlib.suppress(Exception):
            if (await self._open_protective_map()).get(kind):
                return {"ok": True, "verified": True, "existing": True}
        last = code = None
        for attempt in range(1, CONFIG.leg_retry + 1):
            mark = await self.mark_price()
            if self._breached(kind, is_long, mark, trigger):
                return {"ok": False, "breached": True, "mark": mark}
            res = await self._place_one(kind, side, trigger)
            if res.get("ok"):
                if await self._verify_leg(kind):
                    log.info("PROTECT %s VERIFIED trigger=%s orderId=%s latency=%sms attempt=%d",
                             kind.upper(), self.fmt_price(trigger), res.get("orderId"),
                             res.get("latency_ms"), attempt)
                    return {"ok": True, "verified": True, "orderId": res.get("orderId"),
                            "attempts": attempt, "latency_ms": res.get("latency_ms")}
                last, code = "placed_but_not_verified", None
                log.warning("PROTECT %s submit OK tapi TIDAK terverifikasi (attempt %d/%d)",
                            kind.upper(), attempt, CONFIG.leg_retry)
            else:
                last, code = res.get("error"), res.get("code")
                log.warning("PROTECT %s FAILED code=%s msg=%s (attempt %d/%d)",
                            kind.upper(), code, last, attempt, CONFIG.leg_retry)
                if code == -2021:  # would immediately trigger -> mark sudah lewat trigger
                    return {"ok": False, "breached": True, "code": -2021,
                            "error": last, "mark": await self.mark_price()}
            await asyncio.sleep(CONFIG.protect_retry_backoff_sec)
        return {"ok": False, "verified": False, "last_error": last, "code": code}

    async def _arm_protection(self, sl_trigger, tp_trigger, close_side):
        """Inti proteksi. Dipanggil execute (entry), watcher (limit fill), guardian."""
        is_long = (close_side == "SELL")
        amt, entry, mark = await self._wait_position()
        if amt == 0:
            log.error("PROTECT abort: posisi belum aktif setelah %ss (race / entry gagal)",
                      CONFIG.position_wait_timeout_sec)
            return {"ok": False, "attempts": 0, "last_error": "position_not_ready"}
        log.info("PROTECT begin entry=%.4f qty=%s mark=%s SL=%s TP=%s close_side=%s",
                 entry, amt, mark, self.fmt_price(sl_trigger), self.fmt_price(tp_trigger), close_side)

        if self._breached("sl", is_long, mark, sl_trigger):
            r = await self.close_position_market()
            log.warning("PROTECT: SL sudah breached (mark=%s, trigger=%s) -> MARKET close",
                        mark, self.fmt_price(sl_trigger))
            out = {"ok": bool(r.get("ok")), "closed": True, "reason": "sl_breached_preplace", "attempts": 1}
            if not r.get("ok"):
                out["last_error"] = r.get("error")
            return out
        if self._breached("tp", is_long, mark, tp_trigger):
            r = await self.close_position_market()
            log.warning("PROTECT: TP sudah tercapai (mark=%s) -> MARKET close (profit)", mark)
            out = {"ok": bool(r.get("ok")), "closed": True, "reason": "tp_breached_preplace", "attempts": 1}
            if not r.get("ok"):
                out["last_error"] = r.get("error")
            return out

        sl = await self._ensure_leg("sl", close_side, sl_trigger, is_long)
        if sl.get("breached"):
            r = await self.close_position_market()
            return {"ok": bool(r.get("ok")), "closed": True, "reason": "sl_breached",
                    "attempts": sl.get("attempts", 1)}
        if not sl.get("ok"):
            out = {"ok": False, "attempts": CONFIG.leg_retry, "leg_failed": "sl",
                   "last_error": sl.get("last_error"), "code": sl.get("code")}
            if CONFIG.emergency_close_if_unprotected:
                out["emergency_close"] = await self.close_position_market()
            return out

        tp = await self._ensure_leg("tp", close_side, tp_trigger, is_long)
        if tp.get("breached"):
            r = await self.close_position_market()  # SL closePosition auto-cancel saat flat
            return {"ok": bool(r.get("ok")), "closed": True, "reason": "tp_breached", "sl_verified": True}
        return {"ok": True, "attempts": sl.get("attempts", 1),
                "sl_verified": True, "tp_verified": bool(tp.get("ok")),
                "tp_error": None if tp.get("ok") else tp.get("last_error"),
                "tp_code": None if tp.get("ok") else tp.get("code")}

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
        """Wrapper kompatibilitas -> subsistem proteksi baru (_arm_protection)."""
        return await self._arm_protection(sl_trigger, tp_trigger, close_side)

    # ---- GUARDIAN ----
    async def ensure_protection(self, account):
        """Guardian IDEMPOTENT: cek SL & TP terpisah, pasang HANYA yang hilang.
        Tidak pernah menghapus order proteksi yang sudah benar."""
        actions = []
        if not CONFIG.guardian_enabled or CONFIG.dry_run or not CONFIG.binance_api_key:
            return actions
        pos = self.open_position(account)
        if not pos:
            return actions
        try:
            pmap = await self._open_protective_map()
        except Exception as e:
            return [{"market": CONFIG.symbol, "status": "UNVERIFIED", "detail": str(e)}]
        have_sl, have_tp = bool(pmap.get("sl")), bool(pmap.get("tp"))
        if have_sl and have_tp:
            return actions  # sudah terproteksi penuh -> no-op
        entry_px = float(pos.get("entry_price") or 0)
        if entry_px <= 0:
            return [{"market": CONFIG.symbol, "status": "NAKED_NO_ENTRY_PRICE"}]
        is_long = self._position_is_long(pos)
        sp = CONFIG.guardian_stop_pct
        close_side = "SELL" if is_long else "BUY"
        if is_long:
            sl_t, tp_t = entry_px * (1 - sp), entry_px * (1 + sp * CONFIG.min_rr)
        else:
            sl_t, tp_t = entry_px * (1 + sp), entry_px * (1 - sp * CONFIG.min_rr)

        if not have_sl and not have_tp:
            res = await self._arm_protection(sl_t, tp_t, close_side)
            actions.append({"market": CONFIG.symbol, "placed": "both",
                            "status": "PROTECTED" if res.get("ok") else "STILL_NAKED", **res})
            return actions

        missing = "sl" if not have_sl else "tp"
        trig = sl_t if missing == "sl" else tp_t
        leg = await self._ensure_leg(missing, close_side, trig, is_long)
        if leg.get("breached"):
            r = await self.close_position_market()
            actions.append({"market": CONFIG.symbol, "placed": missing,
                            "status": "CLOSED_BREACH" if r.get("ok") else "STILL_NAKED"})
        else:
            row = {"market": CONFIG.symbol, "placed": missing,
                   "status": "PROTECTED" if leg.get("ok") else "STILL_NAKED"}
            if not leg.get("ok"):
                row["last_error"] = leg.get("last_error")
                row["code"] = leg.get("code")
            actions.append(row)
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
            if prot.get("closed"):
                leg = "SL" if "sl" in str(prot.get("reason")) else "TP"
                msg = (f"⚡ <b>Limit terisi, {leg} sudah terpenuhi saat pemasangan → posisi DITUTUP MARKET</b>\n"
                       f"• alasan: {prot.get('reason')}")
            elif prot.get("ok"):
                tp_txt = "SL+TP" if prot.get("tp_verified", True) else "SL (TP gagal, guardian retry)"
                msg = (f"🛡️ <b>Limit terisi → {tp_txt} TERVERIFIKASI</b> (watcher)\n"
                       f"• SL ${decision['stop']:,.1f} · TP ${decision['tp1']:,.1f}")
            elif (prot.get("emergency_close") or {}).get("ok"):
                msg = ("🚨 <b>Limit terisi tapi SL GAGAL → posisi ditutup darurat</b> (reduce-only)\n"
                       f"• Binance code={prot.get('code')} {prot.get('last_error')}")
            else:
                msg = ("⚠️ <b>Limit terisi, SL GAGAL, tutup darurat tak terkonfirmasi — CEK MANUAL!</b>\n"
                       f"• Binance code={prot.get('code')} {prot.get('last_error')}")
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
