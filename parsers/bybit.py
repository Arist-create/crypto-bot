import time
import requests
import websockets
import json
import asyncio
from facades.redis import redis
from facades.mongo import mongo_contract_size
from datetime import datetime
from config import config


bybit_ws = config.BYBIT_WS
bybit_rest_api = config.BYBIT_REST_API
gate_rest_api = config.GATE_REST_API



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
            await redis.set(f"{symbol.replace('USDT', '_USDT')}@BYBIT@index_price", formatted_data)
        askprice, asksize, bidprice, bidsize = data.get("ask1Price"), data.get("ask1Size"), data.get("bid1Price"), data.get("bid1Size")
        if (askprice and asksize and bidprice and bidsize):
            formatted_data = {
                "asks": [{"p": float(data.get("ask1Price")), "s": float(data.get("ask1Size"))}],
                "bids": [{"p": float(data.get("bid1Price")), "s": float(data.get("bid1Size"))}],
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


async def get_pairs():
    response = requests.get(
        f"https://{bybit_rest_api}/v5/market/tickers",
        params={"category": "linear", "baseCoin": "USDT"},
    )

    symbols_bybit = response.json().get("result").get("list")
    
    response = requests.get(
        f"https://{bybit_rest_api}/v5/market/instruments-info",
        params={"category": "linear", "status": "Trading"},
    )
    only_trading = response.json()
    only_trading = {i["symbol"] for i in only_trading.get("result").get("list") if i["symbol"][-4:] == "USDT"}
    symbols_bybit = {i["symbol"].replace("USDT", "_USDT") for i in symbols_bybit if i["symbol"][-4:] == "USDT" and float(i["volume24h"]) > 100000 and i["symbol"] in only_trading}
    # symbols_bybit = {i["symbol"].replace("USDT", "_USDT") for i in symbols_bybit if i["symbol"][-4:] == "USDT" and i["symbol"] in only_trading}
    response = requests.get(f"https://{gate_rest_api}/api/v4/futures/usdt/contracts")
    with_contract_size_gate = response.json()
    with_contract_size_symbols_gate = {i["name"] for i in with_contract_size_gate if i["name"][-4:] == "USDT" and i["in_delisting"] is False}
    print(f"with_contract_size_symbols_gate: {len(with_contract_size_symbols_gate)}")
    response = requests.get(f"https://{gate_rest_api}/api/v4/futures/usdt/tickers")
    symbols_gate = response.json()
    symbols_gate = {
        i["contract"]
        for i in symbols_gate
        if i["contract"][-4:] == "USDT" and i["contract"] in with_contract_size_symbols_gate and int(i["volume_24h_quote"]) > 100000
    }
    # symbols_gate = {
    #     i["contract"]
    #     for i in symbols_gate
    #     if i["contract"][-4:] == "USDT" and i["contract"] in with_contract_size_symbols_gate
    # }
    print(f"Symbols bybit: {symbols_bybit}")
    print(len(symbols_bybit))
    print(f"Symbols gate: {symbols_gate}")
    print(len(symbols_gate))
    symbols = list(symbols_bybit & symbols_gate)
    print(symbols)
    await mongo_contract_size.delete_all()
    await asyncio.gather(
        *[
            redis.set(
                f"{symbol.get('name')}@contract_size",
                float(symbol.get("quanto_multiplier")),
            )
            for symbol in with_contract_size_gate
            if symbol.get("name") in symbols
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
