from facades.redis import redis
import asyncio
from config import config
import requests
from rate_limiter import RateLimiter



mexc_rest_api = config.MEXC_REST_API
gate_rest_api = config.GATE_REST_API
MIN_VOLUME_24H = config.MIN_VOLUME_24H
limiter = RateLimiter(1)

async def get_pairs():
    await limiter.wait()
    response = requests.get(
        f"https://{mexc_rest_api}/api/v1/contract/ticker"
    )
    
    symbols_mexc = response.json().get("data")
    await limiter.wait()
    with_contract_size_mexc = requests.get(
        f"https://{mexc_rest_api}/api/v1/contract/detail"
    )
    with_contract_size_mexc = with_contract_size_mexc.json().get("data")
    with_contract_size_symbols_mexc = {
        i["symbol"]
        for i in with_contract_size_mexc
        if i["symbol"][-4:] == "USDT" and i["state"] == 0 and not i.get("deliveryTime")
    }
  
    symbols_mexc = {
        i["symbol"]
        for i in symbols_mexc
        if i["symbol"][-4:] == "USDT" and float(i["volume24"]) > MIN_VOLUME_24H and i["symbol"] in with_contract_size_symbols_mexc
    }

    await limiter.wait()
    response = requests.get(f"https://{gate_rest_api}/api/v4/futures/usdt/contracts")
    with_contract_size_gate = response.json()
    with_contract_size_symbols_gate = {
        i["name"]
        for i in with_contract_size_gate
        if i["name"][-4:] == "USDT" and i["in_delisting"] is False
    }

    await limiter.wait()
    response = requests.get(f"https://{gate_rest_api}/api/v4/futures/usdt/tickers")
    symbols_gate = response.json()
    symbols_gate = {
        i["contract"]
        for i in symbols_gate
        if i["contract"][-4:] == "USDT"
        and i["contract"] in with_contract_size_symbols_gate
        and int(i["volume_24h_quote"]) > MIN_VOLUME_24H
    }

    symbols = list(symbols_mexc & symbols_gate)

    await asyncio.gather(
        *[
            redis.set(
                f"{symbol.get('name')}@contract_size@GATE",
                float(symbol.get("quanto_multiplier"))
            )
            for symbol in with_contract_size_gate
            if symbol.get("name") in symbols
        ]
    )
    await asyncio.gather(
        *[
            redis.set(
                f"{symbol.get('symbol')}@contract_size@MEXC",
                float(symbol.get("contractSize"))
            )
            for symbol in with_contract_size_mexc
            if symbol.get("symbol") in symbols
        ]
    )
    return symbols
