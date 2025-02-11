import asyncio


class RateLimiter:
    def __init__(self, interval: float):
        self.interval = interval
        self.event = asyncio.Event()
        self.event.set()  # Разрешаем первый вызов

    async def wait(self):
        await self.event.wait()  # Ждем разрешения
        self.event.clear()  # Блокируем следующий вызов
        await asyncio.sleep(self.interval)  # Ждем 2 секунды перед разблокировкой
        self.event.set()  # Разрешаем следующий вызов
