from facades.redis import redis
from datetime import datetime
from parsers.get_list_of_pairs import get_pairs
import time
import asyncio
from facades.mongo import mongo
import traceback
from config import config
from scanner_for_telegram import get_arbs

PERCENT_IN = config.PERCENT_IN if config.PERCENT_IN else 0.0

symbols = None
last_update = None


async def scanner():
    global symbols
    global last_update
    await mongo.delete_all()
    while True:
        if (
            not symbols
            or not last_update
            or (datetime.now() - last_update).total_seconds() > 600
        ):
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
    price_mexc, price_gate, mexc_info, gate_info = await asyncio.gather(
        redis.get(f"{symbol}@MEXC"),
        redis.get(f"{symbol}@GATE"),
        redis.get(f"{symbol}@MEXC@info"),
        redis.get(f"{symbol}@GATE@info"),
    )
    if not price_mexc or not price_gate or not mexc_info or not gate_info:
        return

    funding_rate_mexc, funding_rate_gate = (
        mexc_info.get("funding_rate"),
        gate_info.get("funding_rate"),
    )
    mexc_asks, mexc_bids, gate_asks, gate_bids = (
        price_mexc.get("asks"),
        price_mexc.get("bids"),
        price_gate.get("asks"),
        price_gate.get("bids"),
    )
    if not mexc_asks or not mexc_bids or not gate_asks or not gate_bids:
        return
    mexc_best_bid = mexc_bids[0].get("p")
    gate_best_bid = gate_bids[0].get("p")
    mexc_best_ask = mexc_asks[0].get("p")
    gate_best_ask = gate_asks[0].get("p")

    # if bybit_best_bid > gate_best_ask:
    percent_1 = ((mexc_best_bid - gate_best_ask) / gate_best_ask) * 100
    percent_out_1 = ((mexc_best_ask - gate_best_bid) / gate_best_ask) * 100
    # elif gate_best_bid > bybit_best_ask:
    percent_2 = ((gate_best_bid - mexc_best_ask) / mexc_best_ask) * 100
    percent_out_2 = ((gate_best_ask - mexc_best_bid) / mexc_best_ask) * 100
    return [
        {
            "symbol": symbol,
            "funding_mexc": funding_rate_mexc,
            "funding_gate": funding_rate_gate,
            "percent": percent_1 + funding_rate_mexc - funding_rate_gate,
            "percent_without_funding": percent_1,
            "percent_out": percent_out_1,
            "long": "gate",
            "short": "mexc",
        },
        {
            "symbol": symbol,
            "funding_mexc": funding_rate_mexc,
            "funding_gate": funding_rate_gate,
            "percent": percent_2 + funding_rate_gate - funding_rate_mexc,
            "percent_without_funding": percent_2,
            "percent_out": percent_out_2,
            "long": "mexc",
            "short": "gate",
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
                "funding_mexc": result.get("funding_mexc"),
                "funding_gate": result.get("funding_gate"),
                "percent_without_funding": result.get("percent_without_funding"),
            },
        )
        return
    await mongo.update(
        {"symbol": result.get("symbol"), "long": result.get("long")},
        result,
        upsert=True,
    )

async def main():
    await asyncio.gather(scanner(), get_arbs())

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
