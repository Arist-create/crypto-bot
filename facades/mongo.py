from motor import motor_asyncio


class Mongo:
    def __init__(self, table):
        client = motor_asyncio.AsyncIOMotorClient(
            "mongodb://username:password@mongodb:27017"
        )

        # Создание базы данных
        mydb = client["mydatabase"]

        # Создание коллекции (аналог таблицы в реляционных базах данных)
        self.mycollection = mydb[table]

    async def delete(self, dictionary: dict):
        await self.mycollection.delete_one(dictionary)
    
    async def delete_all(self):
        await self.mycollection.drop()

    async def get_all(self, dictionary: dict = {}):
        return await self.mycollection.find(dictionary).to_list(length=None)

    async def get(self, dictionary: dict):
        return await self.mycollection.find_one(dictionary)

    async def update(
        self, filter_dictionary: dict, dictionary: dict, upsert: bool = False
    ):
        await self.mycollection.update_one(
            filter_dictionary, {"$set": dictionary}, upsert=upsert
        )

    async def count(self, key, value):
        return await self.mycollection.count_documents({f"{key}": value})


mongo = Mongo("mycollection")
mongo_trades = Mongo("trades")
mongo_contract_size = Mongo("contract_size")