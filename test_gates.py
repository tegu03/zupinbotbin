"""Replay deterministik semua gerbang v4 TANPA network. python test_gates.py -> semua PASS."""
import os, sys, asyncio, json, time

os.environ.update({
    "MIN_CONFIDENCE": "65", "MIN_RR": "2.0", "MIN_STOP_PCT": "0.0035",
    "DAILY_LOSS_LIMIT_PCT": "0.03", "DAILY_PROFIT_TARGET_PCT": "0.10",
    "RESUME_HOUR": "0", "STATE_FILE": os.path.join(os.environ.get("TEMP", os.environ.get("TMP", ".")), "zb_test_state.json"),
    "DRY_RUN": "true",
    "WATCH_POLL_SEC": "0.05", "LOOP_MINUTES": "1", "BINANCE_MIN_NOTIONAL": "100",
    "INITIAL_CAPITAL": "5000",
})

from config import CONFIG
from risk import evaluate
import exchange as ex

PASS = []
def check(name, cond):
    print(("PASS  " if cond else "FAIL  ") + name)
    PASS.append(bool(cond)); assert cond, name

def mk(signal, conf, regime, entry=60000, stop=59500, tp1=61200, equity=5000.0, dp=0.0):
    pte = {"signal": signal, "confidence_pct": conf, "regime": regime,
           "entry": {"type": "market", "price": entry}, "invalidation": stop,
           "targets": [tp1, None]}
    mse = {"pte_layer1_input": regime}
    snap = {"account": {"equity_usd": equity, "daily_pnl_pct": dp}}
    return pte, mse, snap

def why(d): return " | ".join(d["reasons"])

# Confidence
d = evaluate(*mk("long", 40, "trending_up"));  check("conf 40 -> DITOLAK", not d["approved"] and "Confidence" in why(d))
d = evaluate(*mk("long", 60, "trending_up"));  check("conf 60 -> DITOLAK", not d["approved"])
d = evaluate(*mk("long", 65, "trending_up"));  check("conf 65 (equity 5000) -> APPROVED", d["approved"])
d = evaluate(*mk("short", 80, "trending_down", 60000, 60600, 58600)); check("conf 80 short/down -> APPROVED", d["approved"])

# Regime alignment
d = evaluate(*mk("short", 80, "trending_up", 60000, 60600, 58600)); check("up + SHORT -> DITOLAK", not d["approved"])
d = evaluate(*mk("long", 80, "trending_down"));                     check("down + LONG -> DITOLAK", not d["approved"])
d = evaluate(*mk("long", 80, "ranging"));                           check("ranging + LONG -> DIIZINKAN (mean reversion)", d["approved"])
d = evaluate(*mk("short", 80, "ranging", 60000, 60600, 58600));     check("ranging + SHORT -> DIIZINKAN (mean reversion)", d["approved"])
d = evaluate(*mk("short", 80, "chop", 60000, 60600, 58600));        check("chop -> DITOLAK", not d["approved"])

# R:R & stop mikro
d = evaluate(*mk("long", 70, "trending_up", 60000, 59500, 60900));  check("R:R 1.8 -> DITOLAK", not d["approved"])
d = evaluate(*mk("long", 70, "trending_up", 60000, 59880, 60300));  check("stop 0.2% -> DITOLAK (mikro)", "mikro" in why(d))

# MIN-NOTIONAL: modal $10 tidak bisa patuh (bukti matematis di gerbang)
d = evaluate(*mk("long", 80, "trending_up", 60000, 59790, 60630, equity=10.0))
check("equity $10 -> DITOLAK: notional < min Binance", not d["approved"] and "minimum Binance" in why(d))
d = evaluate(*mk("long", 80, "trending_up", 60000, 59790, 60630, equity=5000.0))
check("equity $5000, stop 0.35% -> notional lolos min", d["approved"] and d["notional_usd"] > 100)

# Kill switch & profit lock
d = evaluate(*mk("long", 80, "trending_up", dp=-3.1)); check("daily -3.1% -> kill_switch", d["kill_switch"] and not d["approved"])
d = evaluate(*mk("long", 80, "trending_up", dp=-2.0)); check("daily -2.0% -> boleh trade", d["approved"])
d = evaluate(*mk("no_trade", 0, "chop", None, None, None, dp=10.5)); check("daily +10.5% -> profit_lock", d["profit_lock"])

# Latch state harian (UTC)
_sf = CONFIG.state_file
with open(_sf, "w") as f:
    json.dump({"date": ex._today(), "baseline_equity": 5000.0}, f)
ex.latch_kill(-3.5); check("latch_kill -> kill_latched", ex.kill_latched())
with open(_sf, "w") as f:
    json.dump({"date": "2000-01-01", "baseline_equity": 5000.0, "killed_on": "2000-01-01"}, f)
ex._daily_baseline(4900.0); check("hari baru -> latch lepas + baseline reset", not ex.kill_latched())
ex.latch_profit(10.2); check("latch_profit -> profit_latched", ex.profit_latched())

# Resume timer
import main as mn
secs = mn._seconds_until_resume(); check(f"resume dalam rentang (={secs}s)", 0 < secs <= 86460)

# Pembulatan filter Binance
e = ex.Exchange(); e.tick, e.step, e.min_qty = 0.1, 0.001, 0.001
check("fmt_price floor ke tick", e.fmt_price(61743.94) == "61743.9")
qs, qv = e.fmt_qty(0.08169); check("fmt_qty floor ke step", qs == "0.081")

# Watcher: proteksi terpasang saat order FILLED (poll ke-3)
class Fake(ex.Exchange):
    def __init__(self):
        super().__init__(); self.polls = 0; self.protected = None; self.notified = False
    async def order_status(self, oid):
        self.polls += 1
        return {"status": "FILLED" if self.polls >= 3 else "NEW"}
    async def _protect_with_retry(self, sl, tp, close_side):
        self.protected = (sl, tp, close_side); return {"ok": True, "attempts": 1}
    async def _watcher_notify(self, d, p): self.notified = True

f = Fake()
asyncio.run(f._watch_fill({"side": "sell", "stop": 60500.0, "tp1": 59600.0, "entry": 60200.0}, 12345))
check("watcher: FILLED -> SL/TP (close BUY)", f.protected == (60500.0, 59600.0, "BUY") and f.polls >= 3 and f.notified)

class Fake2(Fake):
    async def order_status(self, oid):
        self.polls += 1; return {"status": "CANCELED"}
f2 = Fake2(); t0 = time.time()
asyncio.run(f2._watch_fill({"side": "buy", "stop": 59000.0, "tp1": 61000.0, "entry": 60000.0}, 9))
check("watcher: CANCELED -> berhenti cepat", f2.protected is None and time.time() - t0 < 3)

print(f"\n{sum(PASS)}/{len(PASS)} PASS -- semua gerbang v4 terverifikasi")
