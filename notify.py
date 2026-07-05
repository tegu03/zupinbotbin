"""Telegram notifications (HTML + emoji as STATUS signals, not decoration).

Design choices:
  - parse_mode=HTML so labels can be bold; send() falls back to plain text if a
    message ever contains bad HTML, so a formatting glitch never eats an alert.
  - Emoji encode STATUS at a glance: green/red for PnL sign, checkmark/cross for
    fill vs fail, shield for protected, siren for emergency-close. A failed order
    must never look like a successful one.
  - Dynamic content (errors, reasons) is HTML-escaped. Numbers are safe as-is.
  - tx hashes are shortened; NO-TRADE is collapsed to a few lines to avoid spam.
"""
import httpx
from config import CONFIG


async def send(text, parse_mode="HTML"):
    if not CONFIG.telegram_token or not CONFIG.telegram_chat_id:
        print("[notify] telegram not configured; message:\n", text)
        return
    url = f"https://api.telegram.org/bot{CONFIG.telegram_token}/sendMessage"
    payload = {"chat_id": CONFIG.telegram_chat_id, "text": text, "disable_web_page_preview": True}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    async with httpx.AsyncClient() as c:
        try:
            r = await c.post(url, json=payload, timeout=20)
            if r.status_code != 200 and parse_mode:
                # Most likely an HTML parse error -> resend as plain text so the alert still lands.
                payload.pop("parse_mode", None)
                await c.post(url, json=payload, timeout=20)
        except Exception as e:
            print("[notify] send failed:", e)


# ---- helpers ----
def _esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _f(x):
    try:
        return f"{float(x):,.2f}"
    except (TypeError, ValueError):
        return "n/a"


def _sgn(x):
    try:
        return ("+" if float(x) >= 0 else "") + _f(x)
    except (TypeError, ValueError):
        return "n/a"


def _dot(x):
    try:
        return "🟢" if float(x) >= 0 else "🔴"
    except (TypeError, ValueError):
        return "⚪"


def _short(h):
    h = str(h or "")
    if h.startswith("DRYRUN") or len(h) <= 14:
        return h
    return f"{h[:6]}…{h[-4:]}"


def _header(account):
    a = account
    return "\n".join([
        "🤖 <b>Zupin Bot</b> · BTC Perp <i>(Lighter Testnet)</i>",
        "",
        "💰 <b>Modal &amp; PnL</b>",
        f"• Equity: <b>${_f(a.get('equity_usd'))}</b>  (awal ${_f(a.get('base_capital_usd'))})",
        f"• Unrealized: {_dot(a.get('unrealized_pnl_usd'))} ${_sgn(a.get('unrealized_pnl_usd'))}",
        f"• Hari ini: {_dot(a.get('realized_pnl_today_usd'))} ${_sgn(a.get('realized_pnl_today_usd'))} "
        f"({_sgn(a.get('daily_pnl_pct'))}%)",
    ])


def format_trade(decision, account, exec_result):
    d, e = decision, exec_result
    cfg = CONFIG
    dir_emoji = "📈" if d.get("signal") == "long" else "📉"

    lines = [_header(account), ""]
    lines.append(f"{dir_emoji} <b>ORDER {str(d.get('signal')).upper()}</b> · conf {d.get('confidence_pct')}%")
    lines.append(f"• Regime: {_esc(d.get('regime'))}")
    lines.append(f"• Entry: <b>${_f(d.get('entry'))}</b> ({_esc(d.get('entry_type'))})")
    lines.append(f"• SL 🛑 ${_f(d.get('stop'))}")
    tp = f"• TP 🎯 ${_f(d.get('tp1'))}"
    if d.get("tp2"):
        tp += f" → ${_f(d.get('tp2'))}"
    lines.append(tp)
    lines.append(f"• R:R ⚖️ {d.get('rr')} · risk 💵 ${_f(d.get('risk_usd'))} ({cfg.risk_pct * 100:g}%)")
    lines.append(f"• Size 📦 ${_f(d.get('notional_usd'))} · {d.get('base_amount')} unit")
    lines.append("")

    # --- execution + protection status (the part that must never mislead) ---
    if d.get("dry_run") or e.get("dry_run"):
        lines.append("🧪 <b>DRY-RUN</b> — tidak ada order dikirim")
    elif not e.get("ok"):
        lines.append(f"❌ <b>Entry GAGAL</b>: {_esc(e.get('error', '?'))}")
    else:
        lines.append(f"✅ <b>Entry terisi</b> · tx <code>{_esc(_short(e.get('tx_hash')))}</code>")
        prot = e.get("protection") or {}
        if prot.get("ok"):
            lines.append(f"🛡️ <b>SL/TP terpasang</b> (OCO · percobaan {prot.get('attempts')})")
        elif (prot.get("emergency_close") or {}).get("ok"):
            lines.append("🚨 <b>SL/TP gagal → posisi DITUTUP darurat</b> (reduce-only)")
        elif e.get("warning"):
            lines.append(f"⚠️ <b>{_esc(e.get('warning'))}</b>")
        elif not prot:
            lines.append("ℹ️ SL/TP tidak dipasang (cek PLACE_SL_TP / stop &amp; target)")

    lines.append("")
    lines.append("<i>Bukan nasihat finansial · Testnet</i>")
    return "\n".join(lines)


def format_notrade(decision, account):
    d = decision
    reasons = d.get("reasons", [])
    first = _esc(reasons[0]) if reasons else "-"
    waiting = _esc(d.get("flip_if") or d.get("abstain_reason") or "-")
    icon = "🚫" if "kill switch" in " ".join(reasons).lower() else "⏸️"

    # Full Modal & PnL block on every cycle (so the account is always visible),
    # then a concise no-trade reason so it doesn't get verbose again.
    lines = [_header(account), "",
             f"{icon} <b>NO-TRADE</b> · sinyal {_esc(d.get('signal'))} · conf {d.get('confidence_pct')}%",
             f"• {first}"]
    if waiting and waiting != "-":
        lines.append(f"• Menunggu: {waiting}")
    return "\n".join(lines)


def format_guardian(actions, phase=""):
    icon = {"PROTECTED": "✅", "STILL_NAKED": "⚠️", "UNVERIFIED": "❓",
            "NAKED_NO_ENTRY_PRICE": "⚠️"}
    head = "🛡️ <b>GUARDIAN</b>" + (f" <i>({_esc(phase)})</i>" if phase else "") + " — posisi tanpa proteksi:"
    lines = [head]
    for g in actions:
        st = str(g.get("status", "?"))
        lines.append(f"{icon.get(st, '•')} market {_esc(g.get('market'))}: {_esc(st)}")
    return "\n".join(lines)


def format_online():
    mode = "🧪 DRY-RUN" if CONFIG.dry_run else "🔴 LIVE testnet"
    return f"🤖 <b>Zupin Bot ONLINE</b> · {mode} · loop tiap {CONFIG.loop_minutes} menit"


# ---- SLEEP MODE (posisi terbuka & terproteksi -> AI dijeda untuk hemat token) ----
def format_sleep_enter(account, protected=True):
    prot = "🛡️ SL/TP terverifikasi (OCO)" if protected else "⚠️ proteksi belum terverifikasi (guardian aktif)"
    poll = CONFIG.sleep_poll_sec
    return "\n".join([
        _header(account), "",
        "💤 <b>SLEEP MODE</b> — posisi aktif, AI dijeda (hemat token).",
        f"• {prot}",
        f"• Memantau posisi tiap {poll}s · guardian tiap {CONFIG.guardian_interval_sec // 60} menit.",
        "• Bot akan bangun & cari sinyal lagi saat SL/TP tersentuh.",
    ])


def format_sleep_pnl(account):
    return "\n".join([
        _header(account), "",
        "💤 <b>Masih menunggu SL/TP…</b> — posisi belum tertutup.",
    ])


def format_position_closed(account):
    return "\n".join([
        _header(account), "",
        "✅ <b>Posisi tertutup</b> (SL/TP tersentuh) — bot kembali mencari sinyal.",
    ])
