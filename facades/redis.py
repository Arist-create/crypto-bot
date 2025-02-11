from redis import asyncio as aioredis
import json


class RedisFacade:
    def __init__(self, url):
        self.client = aioredis.from_url(url)

    async def get(self, key) -> dict | None:
        i = await self.client.get(key)
        return json.loads(i) if i else None

    async def set(self, key: str, value):
        return await self.client.set(key, json.dumps(value))

    async def mset(self, dict: dict):
        return await self.client.mset(dict)

    async def delete(self, key):
        return await self.client.delete(key)

    # async def get_all(self):
    #     keys = await self.client.keys()
    #     if not keys:
    #         return []
    #     list = await self.client.mget(keys)
    #     return {json.loads(key): json.loads(i) for key, i in zip(keys, list) if i and key}


redis = RedisFacade("redis://redis:6379/0")
trades_redis = RedisFacade("redis://redis:6379/2")
