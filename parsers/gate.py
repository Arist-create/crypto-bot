import time
import websockets
import json
import asyncio
from facades.redis import redis
from parsers.bybit import get_pairs
from datetime import datetime
from config import config


gate_ws = config.GATE_WS


async def manage_message(websocket):
    k = 0
    while True:
        data = await websocket.recv()
        try:
            data = json.loads(data)
        except Exception as e:
            print(e)
            continue
        pair = data.get("result")
        if not pair:
            print(data)
            continue
        pair = pair.get("contract")
        if not pair:
            print(data)
            continue
        await asyncio.sleep(0.01)
        data = data["result"]
        asks, bids = data.get("asks"), data.get("bids")
        asks, bids = (
            [{"p": float(i["p"]), "s": i["s"]} for i in asks],
            [{"p": float(i["p"]), "s": i["s"]} for i in bids],
        )
        formatted_data = {
            "asks": asks,
            "bids": bids,
            "updatetime": datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"),
        }
        await redis.set(f"{pair}@GATE", formatted_data)
        if k == 200:
            k = 0
            await websocket.send(json.dumps({"channel": "futures.ping"}))
        k += 1


async def get_quote_for_futures(symbols):
    async for websocket in websockets.connect(
        f"wss://{gate_ws}/v4/ws/usdt", ping_interval=5, ping_timeout=None
    ):
        try:
            print("start")
            atime = time.time()
            await asyncio.gather(
                *[
                    websocket.send(
                        json.dumps(
                            {
                                "time": int(atime),
                                "channel": "futures.order_book",
                                "event": "subscribe",
                                "payload": [symbol, "20", "0"],
                            }
                        )
                    )
                    for symbol in symbols
                ]
            )
            print("end")
            await manage_message(websocket)
        except Exception as e:
            print(e)
            await asyncio.sleep(5)
            continue


async def get_index_price(symbols):
    async for websocket in websockets.connect(
        f"wss://{gate_ws}/v4/ws/usdt", ping_interval=5, ping_timeout=None
    ):
        try:
            print("start")
            atime = time.time()
            await asyncio.gather(
                *[
                    websocket.send(
                        json.dumps(
                            {
                                "time": int(atime),
                                "channel": "futures.tickers",
                                "event": "subscribe",
                                "payload": [symbol],
                            }
                        )
                    )
                    for symbol in symbols
                ]
            )
            print("end")
            await manage_message_index_price(websocket)
        except Exception as e:
            print(e)
            await asyncio.sleep(5)
            continue


async def manage_message_index_price(websocket):
    k = 0
    while True:
        data = await websocket.recv()
        try:
            data = json.loads(data)
            data = data.get("result")
            if not data:
                print(data)
                continue
            await asyncio.sleep(0.01)
            for i in data:
                index_price = i.get("index_price")
                funding_rate = i.get("funding_rate")
                formatted_data = {
                    "index_price": float(index_price),
                    "funding_rate": float(funding_rate)*100,
                    "updatetime": datetime.strftime(
                        datetime.now(), "%Y-%m-%d %H:%M:%S"
                    ),
                }
                await redis.set(f"{i.get('contract')}@GATE@info", formatted_data)
            if k == 200:
                k = 0
                await websocket.send(json.dumps({"channel": "futures.ping"}))
            k += 1
        except:
            print(data)


async def main():
    symbols = await get_pairs()
    tasks = [symbols[i : i + 20] for i in range(0, len(symbols), 20)]
    corutines = [get_quote_for_futures(task) for task in tasks]
    corutines.extend([get_index_price(task) for task in tasks])
    await asyncio.gather(*corutines)


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
