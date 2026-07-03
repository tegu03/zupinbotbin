"""DETERMINISTIC Risk Governor v4.1 (Binance). AI mengusulkan; kode ini memutuskan.

Gerbang (SEMUA harus lolos):
  1. Sinyal actionable (long/short)
  2. Regime alignment: trending_up->long, trending_down->short,
     ranging->long/short (mean reversion diizinkan), chop->NO-TRADE
  3. Confidence >= MIN_CONFIDENCE (65)
  4. Geometry valid + R:R >= MIN_RR (2.0)
  5. Stop >= MIN_STOP_PCT (0.35%) -- stop mikro di dalam noise = realized risk >> rencana
  6. Sizing dari STOP (fixed fractional); notional <= max_leverage x equity
  7. NOTIONAL >= minimum Binance (live dari exchangeInfo) -- akun terlalu kecil
     tidak bisa patuh aturan 1%; lebih baik ditolak di sini dengan alasan jelas
     daripada error diam-diam dari exchange
  8. Kill switch: daily <= -3% -> kill_switch=True
  9. Profit lock: daily >= +target -> profit_lock=True
Plus: estimasi fee round-trip (worst case 2x taker) dilaporkan; fee yang memakan
>25% risk diberi peringatan eksplisit -- di akun kecil, fee ADALAH strategi.
"""
from config import CONFIG


def _num(x):
    try:
        v = float(x)
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def evaluate(pte, mse, snapshot):
    cfg = CONFIG
    acc = snapshot.get("account", {})
    equity = _num(acc.get("equity_usd")) or cfg.initial_capital
    reasons, approved = [], True
    kill_switch = False
    profit_lock = False

    signal = pte.get("signal")
    if signal not in ("long", "short"):
        approved = False
        reasons.append(f"Signal not actionable: {signal}")

    regime = mse.get("pte_layer1_input") or pte.get("regime")
    pte_regime = pte.get("regime")
    if pte_regime and regime and pte_regime != regime:
        reasons.append(f"Catatan: regime PTE ({pte_regime}) != MSE ({regime}); dipakai MSE")
    if signal in ("long", "short"):
        if regime == "trending_up" and signal != "long":
            approved = False
            reasons.append("Sinyal SHORT berlawanan regime trending_up -> DITOLAK")
        elif regime == "trending_down" and signal != "short":
            approved = False
            reasons.append("Sinyal LONG berlawanan regime trending_down -> DITOLAK")
        elif regime == "chop":
            approved = False
            reasons.append("Regime chop -> NO-TRADE (market tidak terbaca)")
        elif regime == "ranging":
            reasons.append(f"Regime ranging -> mean reversion DIIZINKAN ({signal})")

    conf = _num(pte.get("confidence_pct"))
    if signal in ("long", "short"):
        if conf is None or conf < cfg.min_confidence:
            approved = False
            reasons.append(f"Confidence {conf if conf is not None else 0:.0f}% < minimum {cfg.min_confidence:.0f}%")

    entry_obj = pte.get("entry") or {}
    entry = _num(entry_obj.get("price"))
    if entry is None:
        zone = entry_obj.get("zone") or [None]
        entry = _num(zone[0])
    stop = _num(pte.get("invalidation"))
    targets = pte.get("targets") or []
    tp1 = _num(targets[0]) if len(targets) > 0 else None
    tp2 = _num(targets[1]) if len(targets) > 1 else None

    if signal in ("long", "short") and (entry is None or stop is None):
        approved = False
        reasons.append("Missing entry or invalidation")

    rr = stop_dist = risk_usd = notional = base_amount = side = fee_est = None
    if entry is not None and stop is not None and tp1 is not None:
        risk_dist = abs(entry - stop)
        reward_dist = abs(tp1 - entry)
        rr = reward_dist / risk_dist if risk_dist > 0 else 0
        if signal == "long" and not (stop < entry < tp1):
            approved = False
            reasons.append("Long geometry invalid (need stop<entry<tp1)")
        if signal == "short" and not (stop > entry > tp1):
            approved = False
            reasons.append("Short geometry invalid (need stop>entry>tp1)")
        if rr < cfg.min_rr:
            approved = False
            reasons.append(f"R:R {rr:.2f} < min {cfg.min_rr}")
        stop_dist = risk_dist / entry if entry > 0 else 0
        if stop_dist < cfg.min_stop_pct:
            approved = False
            reasons.append(f"Stop {stop_dist * 100:.3f}% < minimum {cfg.min_stop_pct * 100:.2f}% (stop mikro)")
        risk_usd = equity * cfg.risk_pct
        notional = risk_usd / stop_dist if stop_dist > 0 else 0
        cap = equity * cfg.max_leverage
        if notional > cap:
            notional = cap
            reasons.append(f"Notional capped at {cfg.max_leverage}x equity")

        # GERBANG MIN-NOTIONAL BINANCE (nilai live menimpa default saat start)
        mn = getattr(CONFIG, "live_min_notional", None) or cfg.binance_min_notional
        if signal in ("long", "short") and mn and notional < mn:
            approved = False
            need = mn * stop_dist / cfg.risk_pct if stop_dist else 0
            reasons.append(f"Notional ${notional:,.2f} < minimum Binance ${mn:,.0f} -- "
                           f"equity minimal untuk stop {stop_dist * 100:.2f}% @ risk {cfg.risk_pct * 100:g}%: "
                           f"${need:,.0f}")

        base_amount = notional / entry if entry > 0 else 0
        side = "buy" if signal == "long" else "sell"
        fee_est = 2 * cfg.taker_fee_pct * notional  # worst case: taker masuk + taker keluar
        if risk_usd and fee_est > 0.25 * risk_usd:
            reasons.append(f"Peringatan fee: est. round-trip ${fee_est:.2f} = "
                           f"{fee_est / risk_usd * 100:.0f}% dari risk -- perlebar stop / pakai limit (maker)")
    elif signal in ("long", "short"):
        approved = False
        reasons.append("Missing TP1 for R:R / sizing")

    ev = str(pte.get("event_risk") or "")
    if ev and any(w in ev.lower() for w in ("high-impact", "imminent", "within hours", "fomc", "cpi", "nfp", "expiry")):
        reasons.append(f"Event risk noted: {ev}")

    dp = _num(acc.get("daily_pnl_pct"))
    if dp is not None and dp <= -(cfg.daily_loss_limit_pct * 100):
        approved = False
        kill_switch = True
        reasons.append(f"KILL SWITCH: daily {dp:.2f}% <= -{cfg.daily_loss_limit_pct * 100:.1f}%")

    if dp is not None and cfg.daily_profit_target_pct > 0 and dp >= cfg.daily_profit_target_pct * 100:
        approved = False
        profit_lock = True
        reasons.append(f"PROFIT LOCK: daily +{dp:.2f}% >= target {cfg.daily_profit_target_pct * 100:.1f}% -> stop entry hari ini")

    if not reasons:
        reasons.append("All gates passed")

    return {
        "approved": bool(approved and signal in ("long", "short")),
        "kill_switch": kill_switch,
        "profit_lock": profit_lock,
        "signal": signal,
        "side": side,
        "regime": regime,
        "confidence_pct": pte.get("confidence_pct"),
        "entry": entry,
        "stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "entry_type": entry_obj.get("type") or "limit",
        "rr": round(rr, 2) if rr is not None else None,
        "stop_distance_pct": round(stop_dist * 100, 3) if stop_dist is not None else None,
        "risk_usd": round(risk_usd, 2) if risk_usd is not None else None,
        "notional_usd": round(notional, 2) if notional is not None else None,
        "base_amount": round(base_amount, 6) if base_amount is not None else None,
        "fee_est_usd": round(fee_est, 4) if fee_est is not None else None,
        "equity_usd": round(equity, 2),
        "dry_run": cfg.dry_run,
        "reasons": reasons,
        "abstain_reason": pte.get("abstain_reason") or "",
        "flip_if": pte.get("flip_if") or "",
        "counter_thesis": pte.get("counter_thesis") or "",
        "funding_note": pte.get("funding_note") or "",
    }
