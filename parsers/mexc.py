import time

from parsers.get_list_of_pairs import get_pairs
import websockets
import json
import asyncio
from facades.redis  import redis
from datetime import datetime
from config import config

MEXC_REST_API = config.MEXC_REST_API
MEXC_WS = config.MEXC_WS
MEXC_API_KEY = config.MEXC_API_KEY
MEXC_SECRET_KEY = config.MEXC_SECRET_KEY


async def manage_message(websocket):
    k = 0
    while True:
        data = await websocket.recv()
        data = json.loads(data)
        pair = data.get("symbol")
        if not pair:
            print(data)
            continue
        await asyncio.sleep(0.01)
        data = data["data"]
        asks, bids = data.get("asks"), data.get("bids")
        asks, bids = [{"p": i[0], "s": i[1]} for i in asks], [{"p": i[0], "s": i[1]} for i in bids]
        formatted_data = {
            "asks": asks,
            "bids": bids, 
            "updatetime": datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
        }
        await redis.set(f"{pair}@MEXC", formatted_data)
        if k == 200:
            k = 0
            await websocket.send('{"method": "ping"}')
        k += 1


async def get_quote_for_futures(symbols):
    async for websocket in websockets.connect(
        f"wss://{MEXC_WS}/edge", ping_interval=5, ping_timeout=300
    ):
        try:
            await asyncio.gather(
                *[
                    websocket.send(
                        json.dumps(
                            {
                                "method": "sub.depth.full",
                                "param": {"symbol": symbol, "limit": 20},
                            }
                        )
                    )
                    for symbol in symbols
                ]
            )
            await manage_message(websocket)
        except Exception as e:
            print(e)
            await asyncio.sleep(5)
            continue

async def get_index_price(symbols):
    async for websocket in websockets.connect(
        f"wss://{MEXC_WS}/edge", ping_interval=5, ping_timeout=300
    ):
        try:
            await asyncio.gather(
                *[
                    websocket.send(
                        json.dumps(
                            {
                                "method": "sub.ticker",
                                "param": {"symbol": symbol},
                            }
                        )
                    )
                    for symbol in symbols
                ]
            )
            await manage_message_index_price(websocket)
        except Exception as e:
            print(e)
            await asyncio.sleep(5)
            continue

async def manage_message_index_price(websocket):
    k = 0
    while True:
        data = await websocket.recv()
        data = json.loads(data)
        pair = data.get("symbol")
        if not pair:
            print(data)
            continue
        await asyncio.sleep(0.01)
        data = data["data"]
        index_price = data.get("indexPrice")
        funding_rate = data.get("fundingRate")
        formatted_data = {
            "index_price": float(index_price), 
            "funding_rate": float(funding_rate)*100,
            "updatetime": datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
        }
        await redis.set(f"{pair}@MEXC@info", formatted_data)
        if k == 200:
            k = 0
            await websocket.send('{"method": "ping"}')
        k += 1


async def main():
    symbols = await get_pairs()
    tasks = [symbols[i : i + 20] for i in range(0, len(symbols), 20)]
    coroutines = [get_quote_for_futures(task) for task in tasks]
    coroutines.extend([get_index_price(task) for task in tasks])
    await asyncio.gather(*coroutines)
    


if __name__ == "__main__":
    while True:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            loop.run_until_complete(main())
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(30)
