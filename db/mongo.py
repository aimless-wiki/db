from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


def get_mongo_client(username: str, password: str) -> MongoClient:

    uri = f"mongodb+srv://{username}:{password}@categories.lqtdyxo.mongodb.net/?retryWrites=true&w=majority"

    client = MongoClient(
        uri,
        server_api=ServerApi("1"),
    )

    return client
