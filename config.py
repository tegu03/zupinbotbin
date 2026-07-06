"""Central config ZupinBot v4 (Binance Futures). Semua dari environment (.env).

Arsitektur tetap: MSE -> PTE -> Risk Governor deterministik -> Executor,
dengan guardian, kill/profit latch, stale-sweep, dan fill-watcher.
Venue baru: Binance USDT-M Futures.
  - EKSEKUSI  : BINANCE_FUTURES_BASE (default TESTNET; ganti 1 baris untuk mainnet)
  - DATA      : selalu mainnet publik (fapi.binance.com) -- funding/OI/LS riil,
                tidak peduli eksekusi di testnet/mainnet.
Filter market (tickSize/stepSize/minNotional) di-fetch live dari exchangeInfo
saat start dan MENIMPA default di bawah (fail-closed jika simbol tak ditemukan).
"""
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


def _f(k, d): return float(os.getenv(k, d))
def _i(k, d): return int(os.getenv(k, d))
def _b(k, d): return os.getenv(k, d).strip().lower() in ("1", "true", "yes", "on")


@dataclass
class Config:
    # --- DeepSeek (AI) ---
    deepseek_api_key: str = os.getenv("DEEPSEEK_API_KEY", "")
    deepseek_base_url: str = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
    model: str = os.getenv("DEEPSEEK_MODEL", "deepseek-v4-pro")
    thinking: bool = _b("DEEPSEEK_THINKING", "true")

    # --- Binance Futures (USDT-M) ---
    binance_base: str = os.getenv("BINANCE_FUTURES_BASE", "https://testnet.binancefuture.com")
    binance_data_base: str = os.getenv("BINANCE_DATA_BASE", "https://fapi.binance.com")
    binance_api_key: str = os.getenv("BINANCE_API_KEY", "")
    binance_api_secret: str = os.getenv("BINANCE_API_SECRET", "")
    symbol: str = os.getenv("SYMBOL", "BTCUSDT")
    recv_window: int = _i("BINANCE_RECV_WINDOW", "5000")
    # Default konservatif; ditimpa nilai LIVE dari exchangeInfo saat start:
    binance_min_notional: float = _f("BINANCE_MIN_NOTIONAL", "100")
    taker_fee_pct: float = _f("TAKER_FEE_PCT", "0.0005")   # 0.05%
    maker_fee_pct: float = _f("MAKER_FEE_PCT", "0.0002")   # 0.02%

    # --- Modal & proteksi ---
    initial_capital: float = _f("INITIAL_CAPITAL", "5000")
    place_sl_tp: bool = _b("PLACE_SL_TP", "true")
    protect_max_retries: int = _i("PROTECT_MAX_RETRIES", "4")
    protect_retry_backoff_sec: float = _f("PROTECT_RETRY_BACKOFF_SEC", "3")
    guardian_enabled: bool = _b("GUARDIAN_ENABLED", "true")
    guardian_stop_pct: float = _f("GUARDIAN_STOP_PCT", "0.01")
    emergency_close_if_unprotected: bool = _b("EMERGENCY_CLOSE_IF_UNPROTECTED", "true")

    # --- Telegram ---
    telegram_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # --- Trading / engine (gerbang identik dgn v3) ---
    interval: str = os.getenv("INTERVAL", "15m")
    risk_pct: float = _f("RISK_PCT", "0.01")
    max_leverage: float = _f("MAX_LEVERAGE", "10")
    min_rr: float = _f("MIN_RR", "2.0")
    min_confidence: float = _f("MIN_CONFIDENCE", "65")
    min_stop_pct: float = _f("MIN_STOP_PCT", "0.0035")
    daily_loss_limit_pct: float = _f("DAILY_LOSS_LIMIT_PCT", "0.03")
    daily_profit_target_pct: float = _f("DAILY_PROFIT_TARGET_PCT", "0.10")
    resume_hour: int = _i("RESUME_HOUR", "0")              # jam UTC; 0 = 07:00 WIB
    block_if_position_open: bool = _b("BLOCK_IF_POSITION_OPEN", "true")
    cancel_stale_entries: bool = _b("CANCEL_STALE_ENTRIES", "true")
    limit_fill_watcher: bool = _b("LIMIT_FILL_WATCHER", "true")
    watch_poll_sec: float = _f("WATCH_POLL_SEC", "5")
    # --- Proteksi SL/TP reliability (v4.2) ---
    position_wait_timeout_sec: float = _f("POSITION_WAIT_TIMEOUT_SEC", "6")
    position_wait_interval_sec: float = _f("POSITION_WAIT_INTERVAL_SEC", "0.4")
    verify_timeout_sec: float = _f("PROTECT_VERIFY_TIMEOUT_SEC", "3")
    verify_interval_sec: float = _f("PROTECT_VERIFY_INTERVAL_SEC", "0.5")
    leg_retry: int = _i("PROTECT_LEG_RETRY", "4")
    # --- Protection mode (v4.3) ---
    # native    = order kondisional exchange (STOP_MARKET/TP_MARKET) -- persist walau bot mati,
    #             TAPI ditolak -4120 di sebagian venue (mis. demo.binance.com)
    # synthetic = SL/TP dipantau bot, tutup MARKET reduceOnly saat harga kena -- jalan di semua
    #             venue, TAPI proteksi HILANG kalau proses bot mati
    # auto      = coba native (closePosition -> reduceOnly+qty); kalau -4120 -> fallback synthetic
    protection_mode: str = os.getenv("PROTECTION_MODE", "auto")
    synth_poll_sec: float = _f("SYNTH_POLL_SEC", "3")
    dry_run: bool = _b("DRY_RUN", "true")
    loop_minutes: int = _i("LOOP_MINUTES", "15")
    notify_every_cycle: bool = _b("NOTIFY_EVERY_CYCLE", "true")
    state_file: str = os.getenv("STATE_FILE", "bot_state.json")
    # --- Laporan performa harian (Telegram) ---
    daily_report_enabled: bool = _b("DAILY_REPORT_ENABLED", "true")
    daily_report_hour_utc: int = _i("DAILY_REPORT_HOUR_UTC", "1")   # 01:00 UTC = 08:00 WIB


CONFIG = Config()
