from facades.mongo import mongo, mongo_trades, mongo_scanned_trades
import requests
from datetime import datetime, timedelta
from telegram_bot import bot, beautify_list_for_telegram
import asyncio
import time
from facades.redis import redis
from rate_limiter import RateLimiter
from trading.module_for_real_trading import real_trade
import traceback
from config import config

CHAT_ID = config.CHAT_ID if config.CHAT_ID else ""
PERCENT_OUT = config.PERCENT_OUT if config.PERCENT_OUT else 0.0
LIFETIME = config.LIFETIME if config.LIFETIME else 0
REAL_TRADE = config.REAL_TRADE if config.REAL_TRADE else 0
MAX_TRADES_COUNT = config.MAX_TRADES_COUNT if config.MAX_TRADES_COUNT else 3
PERCENT_IN = config.PERCENT_IN if config.PERCENT_IN else 0.0
mexc_rest_api = config.MEXC_REST_API
gate_rest_api = config.GATE_REST_API


limiter_tg = RateLimiter(5)
limiter_mexc = RateLimiter(2)
limiter_gate = RateLimiter(2)


async def get_arbs():
    await redis.set("open_positions", True)
    await mongo_trades.delete_all()
    await mongo_scanned_trades.delete_all()
    while True:
        all_pairs = await mongo.get_all()
        await asyncio.gather(*[check_for_arbs(pair) for pair in all_pairs])
        await scanner()


async def scanner():
    top_5_pairs = await mongo_scanned_trades.get_all_sorted(key="percent", direction=-1)
    top_5_pairs = [pair for pair in top_5_pairs]
    message = await beautify_list_for_telegram(top_5_pairs[:5])
    await limiter_tg.wait()
    await bot.edit_message_text(chat_id=CHAT_ID, message_id=12287, text=message)


async def check_for_arbs(pair: dict):
    lifetime = await get_lifetime_arb(pair)
    pair["lifetime"] = lifetime
    del pair["_id"]
    await mongo_scanned_trades.update(
        {"symbol": pair.get("symbol"), "long": pair.get("long")}, pair, upsert=True
    )
    # if pair["min_vol_for_trade_in_usdt"] < 250:
    #     await mongo_scanned_trades.delete(
    #         {"symbol": pair.get("symbol"), "long": pair.get("long")}
    #     )
    #     return

    trade, open_position, trades = await asyncio.gather(
        mongo_trades.get({"symbol": pair.get("symbol"), "long": pair.get("long")}),
        redis.get("open_positions"),
        mongo_trades.get_all(),
    )
    trades_count = len(trades)

    if (
        lifetime > LIFETIME
        and not trade
        and open_position
        and trades_count < MAX_TRADES_COUNT
        and pair["percent_without_funding"] > PERCENT_IN
    ):
        check = await check_for_index_price(pair)
        if not check:
            await mongo_scanned_trades.update(
                {"symbol": pair.get("symbol"), "long": pair.get("long")},
                {"fail_check": "index_price"},
            )
            return
        check = await check_for_volatility(pair)
        if not check:
            await mongo_scanned_trades.update(
                {"symbol": pair.get("symbol"), "long": pair.get("long")},
                {"fail_check": "volatility"},
            )
            return
        check = await check_for_funding_interval(pair)
        if not check:
            await mongo_scanned_trades.update(
                {"symbol": pair.get("symbol"), "long": pair.get("long")},
                {"fail_check": "funding_interval"},
            )
            return
        text = await beautify_list_for_telegram([pair])
        try:
            all = {"CLV_USDT"}
            if REAL_TRADE and pair.get("symbol") not in all:
                check = await real_trade.enter_position(pair)
                if not check:
                    return
            await asyncio.gather(
                mongo_trades.update(
                    {"symbol": pair.get("symbol"), "long": pair.get("long")},
                    pair,
                    upsert=True,
                ),
                limiter_tg.wait(),
            )
            await bot.send_message(chat_id=CHAT_ID, text=text)

        except Exception:
            print(traceback.format_exc())
            pass

    elif lifetime <= LIFETIME and trade and pair["percent_out"] < PERCENT_OUT:
        text = f"Trade for pair {pair.get('symbol')} is closed"
        close_trade = True
        all = {"CLV_USDT"}
        if REAL_TRADE and pair.get("symbol") not in all:
            check = await check_for_volatility(pair)
            if not check:
                return
            check = await real_trade.exit_position(pair)
            if not check:
                return
            position1, position2 = await real_trade._get_position_sizes(pair)
            if position1 != 0 or position2 != 0:
                text = (
                    f"Part size of trade volume for pair {pair.get('symbol')} is closed"
                )
                close_trade = False

        if close_trade:
            await mongo_trades.delete(
                {"symbol": pair.get("symbol"), "long": pair.get("long")}
            )
        await limiter_tg.wait()
        await bot.send_message(
            chat_id=CHAT_ID,
            text=text,
        )


async def get_lifetime_arb(pair: dict):
    start_time = datetime.strptime(pair["start"], "%Y-%m-%d %H:%M:%S")
    current_time = datetime.now()

    # Calculate time difference directly
    return (current_time - start_time).total_seconds()


async def check_for_index_price(pair: dict):
    response = await redis.get(f"{pair.get('symbol')}@MEXC@info")
    if not response:
        return False
    index_price_mexc = response.get("index_price")
    if not index_price_mexc:
        return False

    response = await redis.get(f"{pair.get('symbol')}@GATE@info")
    if not response:
        return False
    index_price_gate = response.get("index_price")
    if not index_price_gate:
        return False
    percent_diff = abs(index_price_mexc - index_price_gate) / index_price_mexc * 100

    return percent_diff < 2


async def check_for_funding_interval(pair: dict):
    symbol = pair.get("symbol")
    await limiter_mexc.wait()
    response = requests.get(
        f"https://{mexc_rest_api}/api/v1/contract/funding_rate/{symbol}"
    )
    mexc_interval = response.json().get("data").get("collectCycle")
    await limiter_gate.wait()
    response = requests.get(f"https://{gate_rest_api}/api/v4/futures/usdt/contracts")
    response = response.json()
    for i in response:
        if i.get("name") == symbol:
            gate_interval = i.get("funding_interval") / 3600
            break

    return mexc_interval == gate_interval


async def check_for_volatility(pair: dict):
    start = datetime.now() - timedelta(minutes=3)
    end = datetime.now()
    await limiter_mexc.wait()
    response = requests.get(
        f"https://{mexc_rest_api}/api/v1/contract/kline/{pair.get('symbol')}?start={int(start.timestamp())}&end={int(end.timestamp())}"
    )
    try:
        data = response.json().get("data")
        low = data.get("realLow")
        lowest = min(low)
        high = data.get("realHigh")
        highest = max(high)
        percent = highest - lowest
        percent = (percent / lowest) * 100
        return percent < 2
    except Exception:
        print(response.json())
        print(traceback.format_exc())
        return False


