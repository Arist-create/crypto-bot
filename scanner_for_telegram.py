from facades.mongo import mongo, mongo_trades
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


limiter_tg = RateLimiter(5)
limiter_volatility = RateLimiter(2)


under_5_min = []
doubles = set()


async def get_arbs():
    await redis.set("open_positions", True)
    await mongo_trades.delete_all()
    while True:
        all_pairs = await mongo.get_all()
        await asyncio.gather(*[check_for_arbs(pair) for pair in all_pairs])


async def scanner():
    while True:
        global under_5_min
        global doubles
        top_5_pairs = sorted(under_5_min, key=lambda x: x.get("percent"), reverse=True)[
            :5
        ]
        under_5_min = []
        doubles = set()
        message = await beautify_list_for_telegram(top_5_pairs)
        await limiter_tg.wait()
        await bot.edit_message_text(chat_id=CHAT_ID, message_id=12287, text=message)


async def check_for_arbs(pair: dict):
    lifetime = await get_lifetime_arb(pair)
    pair["lifetime"] = lifetime
    global under_5_min
    global doubles
    if pair.get("symbol") not in doubles:
        doubles.add(pair.get("symbol"))
        under_5_min.append(pair)
    
    trade, open_position, trades = await asyncio.gather(
            mongo_trades.get({"symbol": pair.get("symbol"), "long": pair.get("long")}),
            redis.get("open_positions"),
            mongo_trades.get_all()
    )
    trades_count = len(trades)

    if lifetime > LIFETIME and not trade and open_position and trades_count < MAX_TRADES_COUNT:
        check = await check_for_index_price(pair)
        fail_check = "Different Index Price"
        pair["fail_check"] = fail_check
        if not check:
            return
        check = await check_for_volatility(pair)
        fail_check = "High Volatility"
        pair["fail_check"] = fail_check
        if not check:
            return
        del pair["fail_check"]
        text = await beautify_list_for_telegram([pair])
        try:
            if REAL_TRADE:
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
        if REAL_TRADE:
            check = await real_trade.exit_position(pair)
            if not check:
                return
            position1, position2 = await real_trade._get_position_sizes(pair)
            if position1 != 0 or position2 != 0:
                text = f"Part size of trade volume for pair {pair.get('symbol')} is closed"
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
    response = await redis.get(f"{pair.get('symbol')}@BYBIT@index_price")
    if not response:
        return False
    index_price_bybit = response.get("index_price")
    if not index_price_bybit:
        return False

    response = await redis.get(f"{pair.get('symbol')}@GATE@index_price")
    if not response:
        return False
    index_price_gate = response.get("index_price")
    if not index_price_gate:
        return False
    percent_diff = abs(index_price_bybit - index_price_gate) / index_price_bybit * 100

    return percent_diff < 1


async def check_for_volatility(pair: dict):
    start = datetime.now() - timedelta(minutes=6)
    end = datetime.now()
    await limiter_volatility.wait()
    response = requests.get(
        f"https://contract.mexc.com/api/v1/contract/kline/{pair.get('symbol')}?start={int(start.timestamp())}&end={int(end.timestamp())}"
    )
    try: 
        data = response.json().get("data")
        low = data.get("realLow")
        lowest = min(low)
        high = data.get("realHigh")
        highest = max(high)
        percent = highest - lowest
        percent = (percent / lowest) * 100
        return percent < 3
    except Exception:
        print(response.json())
        print(traceback.format_exc())
        return False
    

async def main():
    await asyncio.gather(
        get_arbs(), scanner(), real_trade.start_websockets_for_orders()
    )


if __name__ == "__main__":
    while True:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            loop.run_until_complete(main())
        except Exception:
            print(traceback.format_exc())
            time.sleep(5)
