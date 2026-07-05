"""Entry point. ADAPTIVE loop that saves AI tokens while a trade is live.

Two states per iteration:
  - NO open position -> ACTIVE cycle: account -> market data -> snapshot ->
      MSE regime -> PTE thesis -> deterministic Risk Governor -> (execute) -> Telegram.
      This is the only path that calls the AI (classify_regime + analyze_trade).
  - Open & protected  -> SLEEP: the entry/SL/TP plan already lives on the exchange,
      so re-running the model buys nothing. The bot skips ALL AI calls and just
      watches the position (exchange reads are free) until SL or TP closes it,
      then resumes the ACTIVE loop.

Guardian runs every iteration and every GUARDIAN_INTERVAL_SEC while sleeping, so a
position is never left naked. While a position is open the bot NEVER runs the
entry-search AI (prevents stacking a second position on top of the first).
"""
import asyncio
import contextlib
import logging
import time

from config import CONFIG
from data import collect_market_data, build_snapshot
from llm import classify_regime, analyze_trade
from risk import evaluate
from exchange import Exchange
from notify import (send, format_trade, format_notrade, format_guardian,
                    format_online, format_sleep_enter, format_sleep_pnl,
                    format_position_closed)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("pte-bot")


def _has_open_position(account):
    return bool(account.get("positions"))


async def _guardian_sweep(ex, account, phase=""):
    """Run the naked-position guardian and notify if it had to act. Free (no AI)."""
    actions = await ex.ensure_protection(account)
    if actions:
        with contextlib.suppress(Exception):
            await send(format_guardian(actions, phase))
    return actions


async def run_active_cycle(ex, account):
    """Full AI-driven cycle looking for a NEW entry. Returns True if a real order
    was placed (so the caller can switch to sleep immediately, skipping the wait)."""
    raw = await collect_market_data()
    snap = build_snapshot(raw, account)
    log.info("price=%s trend=%s f&g=%s equity=%s",
             snap["price"]["last"], snap["price"]["trend"],
             snap["sentiment"]["fear_greed"], account.get("equity_usd"))

    mse = await classify_regime(snap)
    log.info("regime=%s (%s%%) layer1=%s", mse.get("regime"), mse.get("confidence_pct"), mse.get("pte_layer1_input"))

    pte = await analyze_trade(snap, mse)
    log.info("signal=%s conf=%s rr=%s", pte.get("signal"), pte.get("confidence_pct"), pte.get("rr"))

    decision = evaluate(pte, mse, snap)
    log.info("approved=%s | %s", decision["approved"], " | ".join(decision["reasons"]))

    if decision["approved"]:
        result = await ex.execute(decision)
        log.info("execute -> %s", result)
        await send(format_trade(decision, account, result))
        # A real (non-dry) filled entry means a live position now exists.
        return bool(result.get("ok") and not result.get("dry_run"))
    elif CONFIG.notify_every_cycle:
        await send(format_notrade(decision, account))
    return False


async def sleep_until_flat(ex, account):
    """Bot holds a live, protected trade. Skip ALL AI work; poll the position until
    SL/TP closes it. Guardian and running-PnL notifications continue on their own
    cadences. Every read here is a free exchange call -- no AI tokens spent."""
    protected = await ex.all_positions_protected(account)
    log.info("SLEEP: position open -> pausing AI loop (poll=%ss, protected=%s)",
             CONFIG.sleep_poll_sec, protected)
    with contextlib.suppress(Exception):
        await send(format_sleep_enter(account, protected))

    last_guardian = last_pnl = time.monotonic()
    while True:
        await asyncio.sleep(CONFIG.sleep_poll_sec)
        try:
            account = await ex.get_account()
            account.pop("_raw", None)
        except Exception as e:
            log.warning("sleep poll get_account failed: %s", e)
            continue

        if not _has_open_position(account):
            log.info("WAKE: position closed (SL/TP hit) -> resuming AI loop")
            with contextlib.suppress(Exception):
                await send(format_position_closed(account))
            return

        now = time.monotonic()
        if now - last_guardian >= CONFIG.guardian_interval_sec:
            await _guardian_sweep(ex, account, "sleep")
            last_guardian = now
        if CONFIG.sleep_pnl_notify_sec > 0 and now - last_pnl >= CONFIG.sleep_pnl_notify_sec:
            with contextlib.suppress(Exception):
                await send(format_sleep_pnl(account))
            last_pnl = now


async def main():
    ex = Exchange()
    await ex.start()
    log.info("BOT START | dry_run=%s | model=%s | loop=%dmin | sleep_when_positioned=%s | %s",
             CONFIG.dry_run, CONFIG.model, CONFIG.loop_minutes,
             CONFIG.sleep_when_positioned, CONFIG.lighter_base_url)
    await send(format_online())
    try:
        while True:
            try:
                account = await ex.get_account()
                account.pop("_raw", None)

                # 1) Guardian sweep every loop (free) -- never leave a position naked.
                await _guardian_sweep(ex, account, "loop")

                # 2) Position open? Do NOT run entry-search AI (avoids stacking).
                if _has_open_position(account):
                    if CONFIG.sleep_when_positioned:
                        await sleep_until_flat(ex, account)
                        continue  # position just closed -> re-loop now, skip the long wait
                    log.info("position open; sleep disabled -> skipping entry search this loop")
                else:
                    # 3) Flat -> spend AI to look for an entry.
                    opened = await run_active_cycle(ex, account)
                    if opened:
                        continue  # go straight to sleep-watch (no LOOP_MINUTES wait)
            except Exception as e:
                log.exception("cycle error")
                with contextlib.suppress(Exception):
                    await send(f"Zupin Bot ERROR: {type(e).__name__}: {e}")
            await asyncio.sleep(CONFIG.loop_minutes * 60)
    finally:
        await ex.close()


if __name__ == "__main__":
    asyncio.run(main())
