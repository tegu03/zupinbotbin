"""Verifikasi koneksi & lingkungan Binance SEBELUM menjalankan bot.
Jalankan:  python check_connection.py

Menjawab tiga hal:
  1. exchangeInfo terbaca? (tick/step/minNotional live)
  2. API key valid di venue yang dikonfigurasi? -> saldo tercetak.
     Jika saldo 5.000 USDT sesuai UI demo-mu, akun demo & API testnet SATU sistem.
     Jika auth gagal, ambil API key dari venue yang benar
     (testnet: daftar di https://testnet.binancefuture.com).
  3. Position mode One-way? (bot menolak Hedge mode)
"""
import asyncio
from config import CONFIG
from binance_client import BinanceClient


async def main():
    print(f"Venue eksekusi : {CONFIG.binance_base}")
    print(f"Data market    : {CONFIG.binance_data_base} (mainnet publik)")
    c = BinanceClient()
    await c.start()
    try:
        info = await c.get("/fapi/v1/exchangeInfo")
        sym = next((s for s in info.get("symbols", []) if s.get("symbol") == CONFIG.symbol), None)
        if not sym:
            print(f"GAGAL: simbol {CONFIG.symbol} tidak ada di venue ini.")
            return
        f = {x["filterType"]: x for x in sym.get("filters", [])}
        print(f"exchangeInfo   : OK -- tick={f.get('PRICE_FILTER', {}).get('tickSize')} "
              f"step={f.get('LOT_SIZE', {}).get('stepSize')} "
              f"minQty={f.get('LOT_SIZE', {}).get('minQty')} "
              f"minNotional={(f.get('MIN_NOTIONAL') or f.get('NOTIONAL') or {}).get('notional')}")
        if not CONFIG.binance_api_key:
            print("API key        : belum di-set (.env BINANCE_API_KEY) -- lewati cek akun.")
            return
        bal = await c.sget("/fapi/v2/balance")
        usdt = next((b for b in bal if b.get("asset") == "USDT"), {})
        print(f"Auth           : OK -- saldo USDT = {usdt.get('balance')} "
              f"(available {usdt.get('availableBalance')})")
        dual = await c.sget("/fapi/v1/positionSide/dual")
        mode = "HEDGE (UBAH ke One-way!)" if str(dual.get('dualSidePosition')).lower() == "true" else "One-way (OK)"
        print(f"Position mode  : {mode}")
    except Exception as e:
        print(f"GAGAL: {e}")
        print("Kemungkinan: API key bukan untuk venue ini, futures belum di-enable, "
              "atau IP belum di-whitelist.")
    finally:
        await c.close()


if __name__ == "__main__":
    asyncio.run(main())
