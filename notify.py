# ====== DeepSeek (AI) ======
DEEPSEEK_API_KEY=
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-v4-pro
DEEPSEEK_THINKING=true

# ====== Binance USDT-M Futures ======
# TESTNET (default) -- daftar & buat API key di https://testnet.binancefuture.com
# MAINNET: ganti ke https://fapi.binance.com (JANGAN sebelum sampel testnet hijau)
BINANCE_FUTURES_BASE=https://testnet.binancefuture.com
BINANCE_DATA_BASE=https://fapi.binance.com
BINANCE_API_KEY=
BINANCE_API_SECRET=
SYMBOL=BTCUSDT
BINANCE_RECV_WINDOW=5000
BINANCE_MIN_NOTIONAL=100
TAKER_FEE_PCT=0.0005
MAKER_FEE_PCT=0.0002

# ====== Modal & proteksi ======
INITIAL_CAPITAL=5000
PLACE_SL_TP=true
PROTECT_MAX_RETRIES=4
PROTECT_RETRY_BACKOFF_SEC=3
GUARDIAN_ENABLED=true
GUARDIAN_STOP_PCT=0.01
EMERGENCY_CLOSE_IF_UNPROTECTED=true

# ====== Telegram ======
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

# ====== Trading / engine ======
INTERVAL=15m
RISK_PCT=0.01
MAX_LEVERAGE=10
MIN_RR=2.0
MIN_CONFIDENCE=65
MIN_STOP_PCT=0.0035
DAILY_LOSS_LIMIT_PCT=0.03
DAILY_PROFIT_TARGET_PCT=0.10
RESUME_HOUR=0
BLOCK_IF_POSITION_OPEN=true
CANCEL_STALE_ENTRIES=true
LIMIT_FILL_WATCHER=true
WATCH_POLL_SEC=20
DRY_RUN=true
LOOP_MINUTES=15
NOTIFY_EVERY_CYCLE=true
STATE_FILE=bot_state.json

# ====== Laporan performa harian (Telegram) ======
# Tiap DAILY_REPORT_HOUR_UTC kirim ringkasan trade KEMARIN: jumlah open, menang (TP),
# kalah (SL), win rate, PnL. Sumber: histori income Binance (otoritatif, tahan restart).
DAILY_REPORT_ENABLED=true
DAILY_REPORT_HOUR_UTC=1   # 01:00 UTC = 08:00 WIB
