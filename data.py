"""Market-data v4: SATU venue untuk analisis & eksekusi = Binance.
Data SELALU dari mainnet publik (fapi.binance.com, tanpa API key) — sehingga
funding/OI/long-short adalah crowd RIIL sekalipun eksekusi di testnet/demo.

Endpoint (dokumentasi resmi Binance USDT-M Futures):
  - /fapi/v1/klines                          OHLCV
  - /fapi/v1/premiumIndex                    mark price + lastFundingRate (riil!)
  - /futures/data/openInterestHist           OI historis
  - /futures/data/globalLongShortAccountRatio  rasio akun long/short
  - /futures/data/takerlongshortRatio        taker buy/sell volume ratio
  - alternative.me                           Fear & Greed

Prinsip fail-safe: satu sumber gagal -> None + tercatat di snapshot["data_gaps"];
LLM diminta menurunkan confidence, bukan mengarang angka.
"""
import time
import datetime
import httpx
from config import CONFIG

FNG = "https://api.alternative.me/fng/?limit=1"
_HEADERS = {"User-Agent": "Mozilla/5.0 (zupin-bot)"}
_PERIOD_OK = {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}
_RES_SEC = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600,
            "2h": 7200, "4h": 14400, "6h": 21600, "12h": 43200, "1d": 86400}


def _num(x):
    try:
        v = float(x)
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def _sma(a, n):
    return sum(a[-n:]) / n if len(a) >= n else None


async def _try(client, url, params=None):
    try:
        r = await client.get(url, params=params, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


async def collect_market_data():
    base = CONFIG.binance_data_base.rstrip("/")
    itv = CONFIG.interval if CONFIG.interval in _RES_SEC else "1h"
    period = itv if itv in _PERIOD_OK else "1h"
    sym = CONFIG.symbol
    async with httpx.AsyncClient(headers=_HEADERS) as c:
        klines = await _try(c, f"{base}/fapi/v1/klines",
                            {"symbol": sym, "interval": itv, "limit": 200})
        prem = await _try(c, f"{base}/fapi/v1/premiumIndex", {"symbol": sym})
        oi = await _try(c, f"{base}/futures/data/openInterestHist",
                        {"symbol": sym, "period": period, "limit": 48})
        ls = await _try(c, f"{base}/futures/data/globalLongShortAccountRatio",
                        {"symbol": sym, "period": period, "limit": 24})
        taker = await _try(c, f"{base}/futures/data/takerlongshortRatio",
                           {"symbol": sym, "period": period, "limit": 24})
        fng = await _try(c, FNG)
    return {"klines": klines, "prem": prem, "oi": oi, "ls": ls, "taker": taker, "fng": fng}


def build_snapshot(raw, account):
    gaps = []

    # ---- klines: list of [openTime, o, h, l, c, v, ...] ----
    kl = raw.get("klines") or []
    closes = [v for v in (_num(k[4]) for k in kl if isinstance(k, (list, tuple)) and len(k) > 5) if v is not None]
    highs = [v for v in (_num(k[2]) for k in kl if isinstance(k, (list, tuple)) and len(k) > 5) if v is not None]
    lows = [v for v in (_num(k[3]) for k in kl if isinstance(k, (list, tuple)) and len(k) > 5) if v is not None]
    vols = [v for v in (_num(k[5]) for k in kl if isinstance(k, (list, tuple)) and len(k) > 5) if v is not None]
    if not closes:
        gaps.append("binance_klines")

    last = closes[-1] if closes else None
    per_day = max(1, int(86400 / _RES_SEC.get(CONFIG.interval, 3600)))
    n24 = min(per_day, len(closes)) if closes else 0
    h24 = max(highs[-n24:]) if n24 and highs else None
    l24 = min(lows[-n24:]) if n24 and lows else None
    c24 = closes[-n24 - 1] if len(closes) > n24 else (closes[0] if closes else None)
    chg24 = ((last - c24) / c24 * 100) if (last is not None and c24) else None
    sma20, sma50 = _sma(closes, 20), _sma(closes, 50)
    rng = ((last - l24) / (h24 - l24) * 100) if (last is not None and h24 and l24 and h24 > l24) else None
    trend = "mixed"
    if last is not None and sma20 is not None and sma50 is not None:
        if last > sma20 > sma50:
            trend = "up"
        elif last < sma20 < sma50:
            trend = "down"
    vol_now = sum(vols[-n24:]) if n24 and vols else None
    vol_prev = sum(vols[-2 * n24:-n24]) if vols and len(vols) >= 2 * n24 else None
    volchg = ((vol_now - vol_prev) / vol_prev * 100) if (vol_now is not None and vol_prev) else None

    # ---- premiumIndex: mark + funding riil ----
    prem = raw.get("prem") or {}
    frate = _num(prem.get("lastFundingRate"))
    mark = _num(prem.get("markPrice"))
    if frate is None:
        gaps.append("funding_rate")

    # ---- open interest hist: [{sumOpenInterest, sumOpenInterestValue, timestamp}] ----
    oi_list = raw.get("oi") or []
    oi_last = _num(oi_list[-1].get("sumOpenInterest")) if oi_list else None
    oi_first = _num(oi_list[0].get("sumOpenInterest")) if oi_list else None
    oichg = ((oi_last - oi_first) / oi_first * 100) if (oi_last is not None and oi_first) else None
    if not oi_list:
        gaps.append("open_interest")

    # ---- global long/short account ratio: [{longAccount, shortAccount, longShortRatio}] ----
    ls_list = raw.get("ls") or []
    ls_last = ls_list[-1] if ls_list else {}
    long_pct = _num(ls_last.get("longAccount"))
    ls_ratio = _num(ls_last.get("longShortRatio"))
    if not ls_list:
        gaps.append("long_short_ratio")

    # ---- taker buy/sell ratio: [{buySellRatio, buyVol, sellVol}] ----
    tk_list = raw.get("taker") or []
    taker_ratio = _num(tk_list[-1].get("buySellRatio")) if tk_list else None
    if taker_ratio is None:
        gaps.append("taker_ratio")

    fng = ((raw.get("fng") or {}).get("data") or [{}])[0]
    if _num(fng.get("value")) is None:
        gaps.append("fear_greed")

    venue = "MAINNET" if "fapi.binance.com" in CONFIG.binance_base else "TESTNET/DEMO"
    return {
        "symbol": f"{CONFIG.symbol} Perp (Binance)",
        "execution_venue": venue,
        "interval": CONFIG.interval,
        "as_of": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "data_sources": {"all_market_data": "binance_mainnet_public (crowd riil)"},
        "data_gaps": gaps,
        "price": {
            "last": last, "mark": mark, "change_24h_pct": chg24, "high_24h": h24, "low_24h": l24,
            "range_pos_pct": rng, "sma20": sma20, "sma50": sma50, "trend": trend,
            "volume_24h_base": vol_now, "volume_change_pct": volchg,
        },
        "funding": {"rate": frate,
                    "rate_pct_8h": (frate * 100) if frate is not None else None,
                    "annualized_pct": (frate * 3 * 365 * 100) if frate is not None else None},
        "open_interest": {"current_base": oi_last, "change_window_pct": oichg},
        "long_short": {"account_long_pct": long_pct, "account_ratio": ls_ratio,
                       "taker_buy_sell_ratio": taker_ratio},
        "sentiment": {"fear_greed": _num(fng.get("value")), "label": fng.get("value_classification")},
        "account": account,
    }
