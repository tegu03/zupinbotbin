"""Entry point v4 (Binance Futures). Pipeline per siklus:
  account -> guardian -> kill/profit latch -> sapu order (flat-aware) ->
  position guard -> data mainnet -> MSE -> PTE -> Risk Governor -> execute -> Telegram

Sapu order FLAT-AWARE (penting di Binance, tidak ada OCO native):
  - FLAT  : SEMUA open order disapu -- limit basi + SL/TP yatim dari trade lama.
            SL/TP yatim yang dibiarkan bisa menutup posisi BERIKUTNYA secara salah.
  - POSISI: hanya order non-protektif yang disapu; SL/TP aktif tidak disentuh.

KILL SWITCH: daily <= -3% -> flatten semua + alert -> PAUSE sampai RESUME_HOUR UTC
(0 = 07:00 WIB) -> baseline reset otomatis -> RESUMED. Fail-safe: flatten tidak
terkonfirmasi -> TIDAK tidur; guardian tetap jaga, entry terkunci, minta cek manual.
PROFIT LOCK: daily >= +target -> entry dikunci sampai besok (disiplin, bukan ramalan).
"""
import time
import calendar
import asyncio
import contextlib
import logging

from config import CONFIG
from data import collect_market_data, build_snapshot
from llm import classify_regime, analyze_trade
from risk import evaluate
from exchange import Exchange, kill_latched, latch_kill, profit_latched, latch_profit
from notify import (send, format_trade, format_notrade, format_guardian, format_online,
                    format_position_guard, format_stale_cancel, format_kill_switch,
                    format_sleep, format_resume, format_profit_lock)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("pte-bot")


def _seconds_until_resume():
    now = int(time.time())
    tm = time.gmtime(now)
    today = calendar.timegm((tm.tm_year, tm.tm_mon, tm.tm_mday, CONFIG.resume_hour, 0, 0))
    target = today if now < today else today + 86400
    return max(60, target - now)


def _resume_str():
    h_utc = CONFIG.resume_hour % 24
    h_wib = (h_utc + 7) % 24
    return f"{h_wib:02d}:00 WIB ({h_utc:02d}:00 UTC)"


async def kill_flow(ex: Exchange, account):
    latch_kill(float(account.get("daily_pnl_pct") or 0.0))
    res = await ex.close_all_positions(account)
    log.warning("KILL SWITCH: flatten result=%s", res)
    await send(format_kill_switch(account, res, _resume_str()))
    if res.get("flat") is False:
        await send("⚠️ <b>Flatten TIDAK terkonfirmasi — CEK POSISI MANUAL.</b>\n"
                   "Bot tetap siaga: guardian aktif tiap siklus, entry baru terkunci sampai besok.")
        return
    secs = _seconds_until_resume()
    await send(format_sleep(_resume_str(), secs))
    log.warning("kill switch: sleeping %ss until %s", secs, _resume_str())
    await asyncio.sleep(secs)
    fresh = await ex.get_account()
    await send(format_resume(fresh))


async def run_cycle(ex: Exchange):
    account = await ex.get_account()

    guard = await ex.ensure_protection(account)
    if guard:
        log.warning("guardian actions: %s", guard)
        await send(format_guardian(guard))

    dp = float(account.get("daily_pnl_pct") or 0.0)

    if not CONFIG.dry_run:
        if kill_latched():
            log.info("kill switch latched -> tanpa analisa/entry sampai resume")
            return
        if dp <= -CONFIG.daily_loss_limit_pct * 100:
            await kill_flow(ex, account)
            return
        if profit_latched():
            log.info("profit lock aktif -> tanpa analisa/entry sampai besok")
            return
        if CONFIG.daily_profit_target_pct > 0 and dp >= CONFIG.daily_profit_target_pct * 100:
            latch_profit(dp)
            await send(format_profit_lock(account))
            return

    pos = ex.open_position(account)

    # SAPU ORDER (flat-aware)
    if CONFIG.cancel_stale_entries and not CONFIG.dry_run and CONFIG.binance_api_key:
        if pos is None:
            orders = []
            with contextlib.suppress(Exception):
                orders = await ex.open_orders()
            if orders:
                await ex.sweep_all_orders()
                log.info("flat -> %d open order disapu (entry basi + SL/TP yatim)", len(orders))
                await send(format_stale_cancel(len(orders)))
        else:
            stale = await ex.cancel_entry_orders()
            if stale:
                log.info("stale entry orders canceled: %s", stale)

    # POSITION GUARD
    if pos is not None and CONFIG.block_if_position_open and not CONFIG.dry_run:
        log.info("open position exists -> new entries blocked this cycle")
        if CONFIG.notify_every_cycle:
            await send(format_position_guard(pos, account))
        return

    raw = await collect_market_data()
    snap = build_snapshot(raw, account)
    log.info("price=%s trend=%s f&g=%s equity=%s gaps=%s",
             snap["price"]["last"], snap["price"]["trend"],
             snap["sentiment"]["fear_greed"], account.get("equity_usd"),
             ",".join(snap.get("data_gaps") or []) or "-")

    mse = await classify_regime(snap)
    log.info("regime=%s (%s%%) layer1=%s", mse.get("regime"), mse.get("confidence_pct"),
             mse.get("pte_layer1_input"))

    pte = await analyze_trade(snap, mse)
    log.info("signal=%s conf=%s rr=%s", pte.get("signal"), pte.get("confidence_pct"), pte.get("rr"))

    decision = evaluate(pte, mse, snap)
    log.info("approved=%s | %s", decision["approved"], " | ".join(decision["reasons"]))

    if not CONFIG.dry_run:
        if decision.get("kill_switch"):
            await kill_flow(ex, account)
            return
        if decision.get("profit_lock") and not profit_latched():
            latch_profit(dp)
            await send(format_profit_lock(account))
            return

    if decision["approved"]:
        result = await ex.execute(decision)
        log.info("execute -> %s", result)
        await send(format_trade(decision, account, result))
        fresh = await ex.get_account()
        guard2 = await ex.ensure_protection(fresh)
        if guard2:
            log.warning("post-entry guardian: %s", guard2)
            await send(format_guardian(guard2, phase="pasca-entry"))
    elif CONFIG.notify_every_cycle:
        await send(format_notrade(decision, account))


async def main():
    ex = Exchange()
    await ex.start()
    log.info("BOT START | dry_run=%s | venue=%s | model=%s | loop=%dmin",
             CONFIG.dry_run, CONFIG.binance_base, CONFIG.model, CONFIG.loop_minutes)
    await send(format_online())
    if not CONFIG.dry_run and kill_latched():
        secs = _seconds_until_resume()
        await send(f"⛔ Kill switch masih terkunci — bot tidur sampai {_resume_str()}.")
        await asyncio.sleep(secs)
    try:
        while True:
            try:
                await run_cycle(ex)
            except Exception as e:
                log.exception("cycle error")
                with contextlib.suppress(Exception):
                    await send(f"Zupin Bot ERROR: {type(e).__name__}: {e}")
            await asyncio.sleep(CONFIG.loop_minutes * 60)
    finally:
        await ex.close()


if __name__ == "__main__":
    asyncio.run(main())
