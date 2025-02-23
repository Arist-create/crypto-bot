import time
from uuid import uuid4
import asyncio
import hashlib
import hmac
import requests
import json
from rate_limiter import RateLimiter
from config import config
import websockets

ORDER_QUEUE_GATE = asyncio.Queue()


class GateTrade:
    _gate_api_key = config.GATE_API_KEY
    _gate_secret_key = config.GATE_SECRET_KEY
    _gate_rest_api = config.GATE_REST_API
    _gate_ws = config.GATE_WS
    _leverage = config.LEVERAGE

    def __init__(self):
        self._limiter = RateLimiter(1)

    async def _get_signature(self, method, url, query_string=None, payload_string=None):
        t = time.time()
        m = hashlib.sha512()
        m.update((payload_string or "").encode("utf-8"))
        hashed_payload = m.hexdigest()
        s = "%s\n%s\n%s\n%s\n%s" % (method, url, query_string or "", hashed_payload, t)
        sign = hmac.new(
            self._gate_secret_key.encode("utf-8"), s.encode("utf-8"), hashlib.sha512
        ).hexdigest()
        return {"KEY": self._gate_api_key, "Timestamp": str(t), "SIGN": sign}

    async def _get_signature_for_websocket(self, event, channel, timestamp):
        # In the login channel, `req_param` is always an empty string
        req_param = ""  # Empty string for login request
        
        # Construct the message for signature
        message = f"{event}\n{channel}\n{req_param}\n{timestamp}"

        # Generate the HMAC-SHA512 signature
        signature = hmac.new(
            self._gate_secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha512,
        ).hexdigest()

        return signature

    async def _set_leverage(self, contract, leverage):
        query = f"leverage={leverage}"
        await self._limiter.wait()
        headers = await self._get_signature(
            "POST", f"/api/v4/futures/usdt/positions/{contract}/leverage", query
        )
        response = requests.post(
            f"https://{self._gate_rest_api}/api/v4/futures/usdt/positions/{contract}/leverage?{query}",
            headers=headers,
        )

    async def place_order(self, contract, side, qty, reduce_only=False):
        await self._set_leverage(contract, self._leverage)
        size = qty if side == "long" else -qty
        data = {
            "contract": contract,
            "price": "0.0",
            "size": size,
            "tif": "fok",
            "reduce_only": reduce_only,
        }
        print(f"Open order Gate: {data}")
        # await ORDER_QUEUE_GATE.put(data)

        await self._limiter.wait()
        headers = await self._get_signature(
            "POST", "/api/v4/futures/usdt/orders", payload_string=json.dumps(data)
        )
        response = requests.post(
            f"https://{self._gate_rest_api}/api/v4/futures/usdt/orders",
            data=json.dumps(data),
            headers=headers,
        )
        print(f"Trade Gate: {response.json()}")
        return response.json()

    async def get_balance(self):
        await self._limiter.wait()
        headers = await self._get_signature("GET", "/api/v4/futures/usdt/accounts")
        response = requests.get(
            f"https://{self._gate_rest_api}/api/v4/futures/usdt/accounts",
            headers=headers,
        )
        balance_gate = response.json().get("available")
        print(f"Balance Gate: {balance_gate}")

        return float(balance_gate)

    async def get_all_positions(self):
        await self._limiter.wait()
        query = "holding=true"
        headers = await self._get_signature(
            "GET", "/api/v4/futures/usdt/positions", query
        )
        response = requests.get(
            f"https://{self._gate_rest_api}/api/v4/futures/usdt/positions?{query}",
            headers=headers,
        )
        print(f"Positions Gate: {response.json()}")
        return response.json()

    async def websocket_connection_for_orders(self):
        timestamp = int(time.time())
        signature = await self._get_signature_for_websocket(
            "api", "futures.login", timestamp
        )
        
        payload = {
            "api_key": self._gate_api_key,
            "timestamp": str(timestamp),
            "signature": signature,
            "req_id": "1",
        }
        data = {
            "time": timestamp,
            "channel": "futures.login",
            "event": "api",
            "payload": payload
        }
        async for websocket in websockets.connect(
            f"wss://{self._gate_ws}/v4/ws/usdt"
        ):
            try:
                await websocket.send(json.dumps(data))
                await asyncio.gather(
                    self._sending_orders_in_websocket(websocket),
                    self._sending_ping(websocket),
                    self._read_websocket(websocket)
                )
            except Exception as e:
                print(e)

    async def _sending_orders_in_websocket(self, websocket):
        while True:
            order = await ORDER_QUEUE_GATE.get()
            await websocket.send(
                json.dumps(
                    {
                        "time": int(time.time()),
                        "channel": "futures.order_place",
                        "event": "api",
                        "payload": {
                            "req_id": str(uuid4())[:8],
                            "req_param": order,
                        },
                    }
                )
            )

    async def _sending_ping(self, websocket):
        while True:
            await asyncio.sleep(10)
            await websocket.send(json.dumps({"channel": "futures.ping"}))
    
    async def _read_websocket(self, websocket):
        while True:
            message = await websocket.recv()
            message = json.loads(message)
            print(f"Gate message: {message}")

gate_trade = GateTrade()

