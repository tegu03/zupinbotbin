"""Klien Binance USDT-M Futures tipis di atas httpx. Tanpa SDK eksternal.

Fakta API yang dipakai (dokumentasi resmi fapi):
  - Signed request: HMAC-SHA256 atas query string + header X-MBX-APIKEY.
  - timestamp wajib sinkron: offset diambil dari GET /fapi/v1/time saat start.
  - Error berbentuk {"code": <negatif>, "msg": "..."} -> diangkat sebagai BinanceError.
"""
import time
import hmac
import hashlib
import urllib.parse
import contextlib

import httpx

from config import CONFIG


class BinanceError(RuntimeError):
    def __init__(self, code, msg):
        super().__init__(f"binance {code}: {msg}")
        self.code = code
        self.msg = msg


class BinanceClient:
    def __init__(self, base=None, key=None, secret=None):
        self.base = (base or CONFIG.binance_base).rstrip("/")
        self.key = key if key is not None else CONFIG.binance_api_key
        self.secret = (secret if secret is not None else CONFIG.binance_api_secret).encode()
        self._offset = 0
        self._http = None

    async def start(self):
        self._http = httpx.AsyncClient(
            base_url=self.base, headers={"X-MBX-APIKEY": self.key}, timeout=20)
        with contextlib.suppress(Exception):
            r = await self._http.get("/fapi/v1/time")
            self._offset = int(r.json()["serverTime"]) - int(time.time() * 1000)

    async def close(self):
        if self._http:
            with contextlib.suppress(Exception):
                await self._http.aclose()

    def _ts(self):
        return int(time.time() * 1000) + self._offset

    def _signed_qs(self, params):
        p = {k: v for k, v in (params or {}).items() if v is not None}
        p["timestamp"] = self._ts()
        p["recvWindow"] = CONFIG.recv_window
        q = urllib.parse.urlencode(p)
        sig = hmac.new(self.secret, q.encode(), hashlib.sha256).hexdigest()
        return f"{q}&signature={sig}"

    async def _req(self, method, path, params=None, signed=False):
        try:
            if signed:
                r = await self._http.request(method, f"{path}?{self._signed_qs(params)}")
            else:
                r = await self._http.request(method, path, params=params)
            data = r.json()
        except BinanceError:
            raise
        except Exception as e:
            raise BinanceError("network", f"{type(e).__name__}: {e}")
        if isinstance(data, dict) and isinstance(data.get("code"), int) and data["code"] < 0:
            raise BinanceError(data["code"], data.get("msg", "?"))
        if r.status_code >= 400:
            raise BinanceError(r.status_code, str(data)[:300])
        return data

    async def get(self, path, **params):
        return await self._req("GET", path, params)

    async def sget(self, path, **params):
        return await self._req("GET", path, params, signed=True)

    async def spost(self, path, **params):
        return await self._req("POST", path, params, signed=True)

    async def sdelete(self, path, **params):
        return await self._req("DELETE", path, params, signed=True)
