"""Telegram notifications v4 (Binance). Emoji = sinyal STATUS, bukan dekorasi.
Kejujuran tampilan: order gagal tidak boleh tampak sukses; limit resting BUKAN
'terisi'; venue (TESTNET/MAINNET) selalu terlihat; conf hanya bila bermakna."""
import httpx
from config import CONFIG


def _venue():
    return "MAINNET" if "fapi.binance.com" in CONFIG.binance_base else "TESTNET"


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
                payload.pop("parse_mode", None)
                await c.post(url, json=payload, timeout=20)
        except Exception as e:
            print("[notify] send failed:", e)


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


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _header(account):
    a = account
    return "\n".join([
        f"🤖 <b>Zupin Bot</b> · {CONFIG.symbol} Perp · <b>Binance {_venue()}</b>",
        "",
        "💰 <b>Modal &amp; PnL</b>",
        f"• Equity: <b>${_f(a.get('equity_usd'))}</b>  (awal ${_f(a.get('base_capital_usd'))})",
        f"• Unrealized: {_dot(a.get('unrealized_pnl_usd'))} ${_sgn(a.get('unrealized_pnl_usd'))}",
        f"• Hari ini: {_dot(a.get('realized_pnl_today_usd'))} ${_sgn(a.get('realized_pnl_today_usd'))} "
        f"({_sgn(a.get('daily_pnl_pct'))}%)",
    ])


def format_trade(decision, account, exec_result):
    d, e = decision, exec_result
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
    lines.append(f"• R:R ⚖️ {d.get('rr')} · risk 💵 ${_f(d.get('risk_usd'))} ({CONFIG.risk_pct * 100:g}%)")
    lines.append(f"• Size 📦 ${_f(d.get('notional_usd'))} · {d.get('base_amount')} {CONFIG.symbol[:-4]}")
    if d.get("fee_est_usd") is not None:
        lines.append(f"• Fee est (worst) 🧾 ${_f(d.get('fee_est_usd'))}")
    lines.append("")

    if d.get("dry_run") or e.get("dry_run"):
        lines.append("🧪 <b>DRY-RUN</b> — tidak ada order dikirim")
    elif not e.get("ok"):
        lines.append(f"❌ <b>Entry GAGAL</b>: {_esc(e.get('error', '?'))}")
    else:
        prot = e.get("protection") or {}
        st = e.get("entry_status")
        if st == "resting":
            lines.append(f"⏳ <b>Limit dipasang — BELUM terisi</b> · order <code>{_esc(e.get('tx_hash'))}</code>")
            lines.append("• Disapu otomatis siklus berikutnya jika tak tersentuh")
        elif st == "partial":
            lines.append(f"◔ <b>Limit terisi SEBAGIAN</b> · order <code>{_esc(e.get('tx_hash'))}</code>")
        else:
            lines.append(f"✅ <b>Entry terisi</b> · order <code>{_esc(e.get('tx_hash'))}</code>")
        if prot.get("deferred"):
            if prot.get("mode") == "watcher":
                lines.append(f"🛡️ SL/TP menunggu fill — <b>watcher aktif</b> "
                             f"(cek tiap {int(prot.get('poll_sec') or 20)} dtk)")
            else:
                lines.append("🛡️ SL/TP menunggu fill — guardian memproteksi di siklus berikutnya")
        elif prot.get("ok"):
            lines.append(f"🛡️ <b>SL/TP terpasang</b> (STOP+TP conditional · percobaan {prot.get('attempts')})")
        elif (prot.get("emergency_close") or {}).get("ok"):
            lines.append("🚨 <b>SL/TP gagal → posisi DITUTUP darurat</b> (reduce-only)")
        elif e.get("warning"):
            lines.append(f"⚠️ <b>{_esc(e.get('warning'))}</b>")
        elif not prot:
            lines.append("ℹ️ SL/TP tidak dipasang (cek PLACE_SL_TP / stop &amp; target)")

    lines.append("")
    lines.append(f"<i>Bukan nasihat finansial · Binance {_venue()}</i>")
    return "\n".join(lines)


def format_notrade(decision, account):
    d = decision
    reasons = d.get("reasons", [])
    first = _esc(reasons[0]) if reasons else "-"
    waiting = _esc(d.get("flip_if") or d.get("abstain_reason") or "-")
    joined = " ".join(reasons).lower()
    icon = "🚫" if ("kill switch" in joined or "profit lock" in joined) else "⏸️"
    conf = _num(d.get("confidence_pct"))
    conf_txt = f" · conf {conf:.0f}%" if (conf is not None and conf > 0) else ""
    lines = [_header(account), "",
             f"{icon} <b>NO-TRADE</b> · sinyal {_esc(d.get('signal'))}{conf_txt}",
             f"• {first}"]
    if waiting and waiting != "-":
        lines.append(f"• Menunggu: {waiting}")
    return "\n".join(lines)


def format_guardian(actions, phase=""):
    icon = {"PROTECTED": "✅", "STILL_NAKED": "⚠️", "UNVERIFIED": "❓", "NAKED_NO_ENTRY_PRICE": "⚠️"}
    head = "🛡️ <b>GUARDIAN</b>" + (f" <i>({_esc(phase)})</i>" if phase else "") + " — posisi tanpa proteksi:"
    lines = [head]
    for g in actions:
        st = str(g.get("status", "?"))
        lines.append(f"{icon.get(st, '•')} {_esc(g.get('market'))}: {_esc(st)}")
    return "\n".join(lines)


def format_online():
    mode = "🧪 DRY-RUN" if CONFIG.dry_run else f"🔴 LIVE {_venue()}"
    return (f"🤖 <b>Zupin Bot ONLINE</b> · {mode} · {CONFIG.symbol} · loop {CONFIG.loop_minutes} menit\n"
            f"• Eksekusi: Binance {_venue()} · Data: Binance MAINNET (funding/OI/LS riil) + F&amp;G\n"
            f"• Gerbang: conf ≥ {CONFIG.min_confidence:g}% · R:R ≥ {CONFIG.min_rr:g} · "
            f"stop ≥ {CONFIG.min_stop_pct * 100:.2f}% · trend-only · min-notional aware\n"
            f"• Kill switch: -{CONFIG.daily_loss_limit_pct * 100:g}%/hari → flatten + pause s.d. "
            f"{(CONFIG.resume_hour + 7) % 24:02d}:00 WIB · Profit lock: +{CONFIG.daily_profit_target_pct * 100:g}%")


def format_position_guard(pos, account):
    lines = [_header(account), ""]
    lines.append("⏸️ <b>Posisi masih terbuka — entry baru diblokir</b> (guard)")
    lines.append(f"• Size: {_esc(pos.get('size'))} @ ${_f(pos.get('entry_price'))}")
    lines.append("• SL/TP di exchange tetap aktif · bot menunggu posisi selesai")
    lines.append("")
    lines.append(f"<i>Bukan nasihat finansial · Binance {_venue()}</i>")
    return "\n".join(lines)


def format_stale_cancel(n):
    return (f"🧹 <b>Order lama disapu</b> · {n} order (limit basi / SL-TP yatim)\n"
            "• Papan bersih sebelum siklus baru — tesis lama tidak dibawa")


def format_kill_switch(account, res, resume_str):
    ok_close = sum(1 for c in (res.get("closed") or []) if c.get("ok"))
    flat = res.get("flat")
    lines = [_header(account), "",
             "🚨 <b>KILL SWITCH ACTIVATED</b>",
             f"• Daily loss: {_sgn(account.get('daily_pnl_pct'))}% (limit: -{CONFIG.daily_loss_limit_pct * 100:.2f}%)",
             f"• Semua order disapu: {'ya' if res.get('canceled') else 'gagal/cek'} · posisi ditutup: {ok_close}",
             f"• Status flat: {'✅ ya' if flat else ('❓ belum terkonfirmasi' if flat is None else '❌ TIDAK — cek manual')}",
             f"• Bot PAUSE sampai {resume_str}",
             "", f"<i>Bukan nasihat finansial · Binance {_venue()}</i>"]
    return "\n".join(lines)


def format_sleep(resume_str, secs):
    return f"💤 Bot tidur · lanjut {resume_str} (±{secs / 3600:.1f} jam)"


def format_resume(account):
    return "\n".join([_header(account), "",
                      "✅ <b>Bot RESUMED</b> — baseline harian di-reset, kill switch terbuka"])


def format_profit_lock(account):
    lines = [_header(account), "",
             "🎯 <b>TARGET HARIAN TERCAPAI — PROFIT LOCK</b>",
             f"• Hari ini {_sgn(account.get('daily_pnl_pct'))}% ≥ target +{CONFIG.daily_profit_target_pct * 100:g}%",
             "• Entry baru dikunci sampai besok · posisi berjalan tetap dikelola SL/TP + guardian",
             "", f"<i>Bukan nasihat finansial · Binance {_venue()}</i>"]
    return "\n".join(lines)
