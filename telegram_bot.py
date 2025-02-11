from facades.mongo import mongo

from aiogram import Bot, Dispatcher
from aiogram import types, executor
import time
from datetime import datetime
from facades.redis import redis



bot = Bot(token='7473932480:AAHvJvYndS0-blMx8U-w57BBjMuUTl01E7E')

dp = Dispatcher(bot)


@dp.message_handler(commands=["start"])
async def send_welcome(message: types.Message):
    await message.reply(f"Chat ID: {message.chat.id}, message ID: {message.message_id}")

@dp.message_handler(commands=["open"])
async def open_new_orders_status(message: types.Message):
    await redis.set("open_positions", True)
    await message.reply("Opened")

@dp.message_handler(commands=["check"])
async def check_new_orders_status(message: types.Message):
    open_position = await redis.get("open_positions")
    await message.reply(f"Open: {open_position}")

@dp.message_handler(commands=["close"])
async def close_new_orders_status(message: types.Message):
    await redis.set("open_positions", False)
    await message.reply("Closed")

@dp.message_handler(content_types=["text"])
async def echo(message: types.Message):
    pair = await mongo.get_all({"symbol": message.text})
    text = "Not found"
    if pair:
        text = await beautify_list_for_telegram(pair)
    await bot.edit_message_text(chat_id=message.chat.id, message_id=12321, text=text)
    await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)


async def beautify_list_for_telegram(arbs: list):
    result = []
    for pair in arbs:
        result.append(f'{pair.get("symbol")}\n')
        result.append(f'Percent: {pair.get("percent"):.2f}\n')
        result.append(f'Percent out: {pair.get("percent_out"):.2f}\n')
        if pair.get("lifetime"):
            result.append(f'Lifetime: {(pair.get("lifetime") / 60):.2f}\n')
        result.append(f'Long: {pair.get("long")}\n')
        result.append(f'Short: {pair.get("short")}\n\n')
        if pair.get("fail_check"):
            result.append(f'Fail check: {pair.get("fail_check")}\n\n')
    result.append(f'Last update: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    return "".join(result)


if __name__ == "__main__":
    while True:
        try:
            executor.start_polling(dp, skip_updates=True)
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)
