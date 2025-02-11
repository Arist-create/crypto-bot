import time
import asyncio
import hashlib
import hmac
import requests
import json
from rate_limiter import RateLimiter
from config import config
import websockets

ORDER_QUEUE_BYBIT = asyncio.Queue()



class BybitTrade:
    _recv_window = "5000"

    _bybit_rest_api = config.BYBIT_REST_API
    _bybit_ws = config.BYBIT_WS
    _bybit_api_key = config.BYBIT_API_KEY
    _bybit_secret_key = config.BYBIT_SECRET_KEY
    _leverage = config.LEVERAGE

    def __init__(self):
        self._limiter = RateLimiter(1)

    async def _get_signature(self, payload=""):
        time_stamp = str(int(time.time() * 10**3))
        param_str = str(time_stamp) + self._bybit_api_key + self._recv_window + payload
        hash = hmac.new(
            bytes(self._bybit_secret_key, "utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256,
        )
        signature = hash.hexdigest()
        headers = {
            "Content-Type": "application/json",
            "X-BAPI-API-KEY": self._bybit_api_key,
            "X-BAPI-TIMESTAMP": time_stamp,
            "X-BAPI-RECV-WINDOW": self._recv_window,
            "X-BAPI-SIGN": signature,
            "X-BAPI-SIGNTYPE": "2",
        }
        return headers

    async def _set_leverage(self, contract, leverage):
        symbol = contract.replace("_USDT", "USDT")
        data = {
            "category": "linear",
            "symbol": symbol,
            "buyLeverage": str(leverage),
            "sellLeverage": str(leverage),
        }
        await self._limiter.wait()
        headers = await self._get_signature(json.dumps(data))
        response = requests.post(
            f"https://{self._bybit_rest_api}/v5/position/set-leverage",
            data=json.dumps(data),
            headers=headers,
        )

    async def get_balance(self):
        params = "accountType=UNIFIED"
        await self._limiter.wait()
        headers = await self._get_signature(params)
        response = requests.get(
            f"https://{self._bybit_rest_api}/v5/account/wallet-balance?{params}",
            headers=headers,
        )
        data = response.json()
        balance_bybit = float(
            data.get("result").get("list")[0].get("totalAvailableBalance")
        )
        return balance_bybit

    async def place_order(self, contract, side, qty, reduce_only=False):
        await self._set_leverage(contract, self._leverage)
        contract = contract.replace("_USDT", "USDT")
        side = "Buy" if side == "long" else "Sell"
        qty = int(qty)
        data = {
            "category": "linear",
            "symbol": contract,
            "side": side,
            "orderType": "Market",
            "qty": str(qty),
            "reduceOnly": reduce_only,
        }
        
        print(f"Open order Bybit: {data}") 
        await ORDER_QUEUE_BYBIT.put(data)

        # await self._limiter.wait()
        # headers = await self._get_signature(json.dumps(data))
        # response = requests.post(
        #     f"https://{self._bybit_rest_api}/v5/order/create",
        #     json=data,
        #     headers=headers,
        # )
        # print(f"Trade Bybit: {response.json()}")
        # return response.json()

    async def get_all_positions(self):
        params = "category=linear&settleCoin=USDT"
        await self._limiter.wait()
        headers = await self._get_signature(params)
        response = requests.get(
            f"https://{self._bybit_rest_api}/v5/position/list?{params}", headers=headers
        )
        response = response.json()
        response = response.get("result").get("list")

        return response

    async def websocket_connection_for_orders(self):
        expires = int((time.time() + 5) * 1000)
        signature = await self._get_signature_for_websocket(expires)
        payload = {"req_id": 1, "op": "auth", "args": [self._bybit_api_key, expires, signature]}
        async for websocket in websockets.connect(f"wss://{self._bybit_ws}/v5/trade"):
            try:
                await websocket.send(json.dumps(payload))
                await asyncio.gather(
                    self._sending_orders_in_websocket(websocket),
                    self._sending_ping(websocket),
                    self._read_websocket(websocket),
                )
            except Exception as e:
                print(e)

    async def _get_signature_for_websocket(self, expires):
        # Generate signature.
        signature = str(
            hmac.new(
                bytes(self._bybit_secret_key, "utf-8"),
                bytes(f"GET/realtime{expires}", "utf-8"),
                digestmod="sha256",
            ).hexdigest()
        )
        return signature

    async def _sending_orders_in_websocket(self, websocket):
        while True:
            order = await ORDER_QUEUE_BYBIT.get()
            await websocket.send(
                json.dumps(
                    {
                        "req_id": 1,
                        "header": {
                            "X-BAPI-TIMESTAMP": f"{int(time.time() * 1000)}",
                            "X-BAPI-RECV-WINDOW": "5000",
                        },
                        "op": "order.create",
                        "args": [order],
                    }
                )
            )

    async def _sending_ping(self, websocket):
        while True:
            await asyncio.sleep(10)
            await websocket.send(json.dumps({"op": "ping"}))

    async def _read_websocket(self, websocket):
        while True:
            message = await websocket.recv()
            message = json.loads(message)
            print(f"Bybit message: {message}")

bybit_trade = BybitTrade()