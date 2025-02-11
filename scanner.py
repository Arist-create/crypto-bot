from facades.redis import redis
from datetime import datetime
from parsers.bybit import get_pairs
import time
import asyncio
from facades.mongo import mongo
import traceback
from config import config

PERCENT_IN = config.PERCENT_IN if config.PERCENT_IN else 0.0

symbols = None
last_update = None


async def main():
    global symbols
    global last_update
    await mongo.delete_all()
    while True:
        if not symbols or not last_update or (datetime.now() - last_update).total_seconds() > 600:
            symbols = await get_pairs()
            last_update = datetime.now()
        results = await asyncio.gather(*[compare_prices(symbol) for symbol in symbols])
        arr = [item for result in results if result for item in result]

        await asyncio.gather(*[put_in_db(result) for result in arr])
        all = await mongo.get_all()
        arr = {result.get("symbol") for result in arr}

        await asyncio.gather(
            *[
                mongo.delete({"symbol": pair.get("symbol")})
                for pair in all
                if pair.get("symbol") not in arr
            ]
        )

async def compare_prices(symbol: str):
    price_bybit, price_gate, contract_size = await asyncio.gather(
        redis.get(f"{symbol}@BYBIT"),
        redis.get(f"{symbol}@GATE"),
        redis.get(f"{symbol}@contract_size"),
    )
    if not price_bybit or not price_gate:
        return
    bybit_asks, bybit_bids, gate_asks, gate_bids = price_bybit.get("asks"), price_bybit.get("bids"), price_gate.get("asks"), price_gate.get("bids")
    if not bybit_asks or not bybit_bids or not gate_asks or not gate_bids:
        return
    bybit_best_bid = bybit_bids[0].get("p")
    gate_best_bid = gate_bids[0].get("p")
    bybit_best_ask = bybit_asks[0].get("p")
    gate_best_ask = gate_asks[0].get("p")
    gate_size_ask = gate_asks[0].get("s")
    gate_size_bid = gate_bids[0].get("s")
    bybit_size_ask = bybit_asks[0].get("s")
    bybit_size_bid = bybit_bids[0].get("s")

    # if bybit_best_bid > gate_best_ask:
    percent_1 = ((bybit_best_bid - gate_best_ask) / gate_best_ask) * 100
    percent_out_1 = ((bybit_best_ask - gate_best_bid) / gate_best_ask) * 100
    # elif gate_best_bid > bybit_best_ask:
    percent_2 = ((gate_best_bid - bybit_best_ask) / bybit_best_ask) * 100
    percent_out_2 = ((gate_best_ask - bybit_best_bid) / bybit_best_ask) * 100
    return [
        {
            "symbol": symbol,
            "percent": percent_1,
            "percent_out": percent_out_1,
            "long": "gate",
            "short": "bybit",
            "min_vol_for_trade_in_usdt": min(bybit_best_bid*bybit_size_bid, gate_best_ask*gate_size_ask*contract_size),
        },
        {
            "symbol": symbol,
            "percent": percent_2,
            "percent_out": percent_out_2,
            "long": "bybit",
            "short": "gate",
            "min_vol_for_trade_in_usdt": min(bybit_best_ask*bybit_size_ask, gate_best_bid*gate_size_bid*contract_size),
        },
    ]


async def put_in_db(result: dict):
    result["start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pair = await mongo.get({"symbol": result.get("symbol"), "long": result.get("long")})
    if pair:
        if result["percent"] > PERCENT_IN:
            result["start"] = pair.get("start")
        await mongo.update(
            {"symbol": result.get("symbol"), "long": result.get("long")},
            {
                "percent": result.get("percent"),
                "start": result.get("start"),
                "percent_out": result.get("percent_out"),
            },
        )
        return
    await mongo.update(
        {"symbol": result.get("symbol"), "long": result.get("long")},
        result,
        upsert=True,
    )
    


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
            print(traceback.format_exc())
            time.sleep(5)
