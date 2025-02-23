import time
import asyncio
import hashlib
import hmac
import requests
from curl_cffi import requests as requests_curl

import json
from rate_limiter import RateLimiter
from config import config



class MexcTrade:
    _leverage = config.LEVERAGE
    _authorization_key = config.MEXC_AUTHORIZATION_KEY
    _rest_api = config.MEXC_REST_API
    _api_key = config.MEXC_API_KEY
    _secret_key = config.MEXC_SECRET_KEY

    def __init__(self):
        self._limiter = RateLimiter(1)

    def _md5(self, value):
        return hashlib.md5(value.encode('utf-8')).hexdigest()
    
    async def _get_signature(self, **kwargs):
        timestamp = str(int(time.time() * 1000))
        query_string = "&".join([f"{k}={v}" for k, v in sorted(kwargs.items())])
        query_string = self._api_key + timestamp + query_string
        signature = hmac.new(
            self._secret_key.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        headers = {"APIKEY": self._api_key, "Request-Time": timestamp, "Signature": signature}
        return headers

    async def _get_signature_for_trade(self, key, obj):
        date_now = str(int(time.time() * 1000))  
        g = self._md5(key + date_now)[7:] 
        s = json.dumps(obj, separators=(',', ':'))  
        sign = self._md5(date_now + s + g)  
        return {'time': date_now, 'sign': sign}

    async def place_order(self, contract, side, qty, reduce_only=False):
        if side == "long" and not reduce_only:
            side = 1
        elif side == "short" and not reduce_only:
            side = 3
        elif side == "long" and reduce_only:
            side = 2
        elif side == "short" and reduce_only:
            side = 4
        data = {
            "symbol": contract,
            "price": 0.0,
            "type": "5",
            "openType": 1,
            "vol": qty,
            "side": side,
            "leverage": self._leverage,
            "reduceOnly": reduce_only,
            "priceProtect": "0"
        }
        print(f"Open order Mexc: {data}")

        signature = await self._get_signature_for_trade(
            self._authorization_key, data)
        headers = {
            'Content-Type': 'application/json',
            'x-mxc-sign': signature['sign'],
            'x-mxc-nonce': signature['time'],
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            'Authorization': self._authorization_key
        }
        await self._limiter.wait()
        response = requests_curl.post("https://futures.mexc.com/api/v1/private/order/create", headers=headers, json=data)
        print(f"Trade Mexc: {response.json()}")
        return response.json()

    async def get_balance(self):
        await self._limiter.wait()
        headers = await self._get_signature()
        response = requests.get(
            f"https://{self._rest_api}/api/v1/private/account/asset/USDT",
            headers=headers,
        )
        balance_mexc = response.json().get("data").get("availableBalance")
        print(f"Balance Mexc: {balance_mexc}")

        return float(balance_mexc)

    async def get_all_positions(self):
        await self._limiter.wait()
        headers = await self._get_signature()
        response = requests.get(
            f"https://{self._rest_api}/api/v1/private/position/open_positions",
            headers=headers,
        )
        print(f"Positions Mexc: {response.json()}")
        return response.json().get("data")

mexc_trade = MexcTrade()