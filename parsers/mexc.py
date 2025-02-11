import time

import requests
import websockets
import json
import asyncio
from facades.redis  import redis
from facades.mongo import mongo_contract_size
from datetime import datetime


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
        "wss://contract.mexc.com/edge", ping_interval=5, ping_timeout=300
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
        "wss://contract.mexc.com/edge", ping_interval=5, ping_timeout=300
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
        formatted_data = {
            "index_price": float(index_price), 
            "updatetime": datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
        }
        await redis.set(f"{pair}@MEXC@index_price", formatted_data)
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
    


async def get_pairs():
    response = requests.get(
        "https://contract.mexc.com/api/v1/contract/ticker"
    )

    symbols_mexc = response.json().get("data")
    symbols_mexc = {
        i["symbol"]
        for i in symbols_mexc
        if i["symbol"][-4:] == "USDT" and i["volume24"] > 300000
    }
    response = requests.get(
        "https://api.gateio.ws/api/v4/futures/usdt/tickers"
    )
    symbols_gate = response.json()
    symbols_gate = {
        i["contract"]
        for i in symbols_gate
        if i["contract"][-4:] == "USDT" and int(i["volume_24h_quote"]) > 300000
    }
    symbols = list(symbols_mexc & symbols_gate)
    response = requests.get(
        "https://contract.mexc.com/api/v1/contract/detail"
    )
    data = response.json().get("data")
    await mongo_contract_size.delete_all()
    await asyncio.gather(
        *[
            mongo_contract_size.update({"symbol": symbol.get("symbol")}, {
                "symbol": symbol.get("symbol"),
                "contract_size": symbol.get("contractSize")
            }, True)
            for symbol in data if symbol.get("symbol") in symbols
        ]
    )
    return symbols


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
