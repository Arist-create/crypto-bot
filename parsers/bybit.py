import time
import websockets
import json
import asyncio
from facades.redis import redis
from parsers.get_list_of_pairs import get_pairs
from datetime import datetime
from config import config


bybit_ws = config.BYBIT_WS
bybit_rest_api = config.BYBIT_REST_API
gate_rest_api = config.GATE_REST_API
MIN_VOLUME_24H = config.MIN_VOLUME_24H


async def get_index_price(symbols):
    async for websocket in websockets.connect(
        f"wss://{bybit_ws}/v5/public/linear", ping_interval=5, ping_timeout=300
    ):
        try:
            await asyncio.gather(
                *[
                    websocket.send(
                        json.dumps(
                            {
                                "op": "subscribe",
                                "args": [f"tickers.{symbol.replace('_', '')}"],
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
        data = data.get("data")
        if not data:
            print(data)
            continue
        await asyncio.sleep(0.01)
        symbol = data.get("symbol")
        if not symbol:
            print(data)
            continue
        index_price = data.get("indexPrice")
        if index_price:
            formatted_data = {
                "index_price": float(index_price),
                "updatetime": datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"),
            }
            await redis.set(
                f"{symbol.replace('USDT', '_USDT')}@BYBIT@index_price", formatted_data
            )
        askprice, asksize, bidprice, bidsize = (
            data.get("ask1Price"),
            data.get("ask1Size"),
            data.get("bid1Price"),
            data.get("bid1Size"),
        )
        if askprice and asksize and bidprice and bidsize:
            formatted_data = {
                "asks": [
                    {
                        "p": float(data.get("ask1Price")),
                        "s": float(data.get("ask1Size")),
                    }
                ],
                "bids": [
                    {
                        "p": float(data.get("bid1Price")),
                        "s": float(data.get("bid1Size")),
                    }
                ],
                "updateTime": datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"),
            }
            await redis.set(f"{symbol.replace('USDT', '_USDT')}@BYBIT", formatted_data)
        if k == 200:
            k = 0
            await websocket.send('{"op": "ping"}')
        k += 1


async def main():
    symbols = await get_pairs()
    tasks = [symbols[i : i + 20] for i in range(0, len(symbols), 20)]
    coroutines = [get_index_price(task) for task in tasks]
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
