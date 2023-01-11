from pymongo import MongoClient


def singleton(cls):
    instances = {}

    def wrapper(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return wrapper


@singleton
class Database:
    db = None
    client = None

    def __init__(self, host, port, username, password, authSource, authMechanism, database):
        self.client = MongoClient(
            host=host,
            port=port,
            username=username,
            password=password,
            authSource=authSource,
            authMechanism=authMechanism
        )
        self.db = self.client[database]

    def __del__(self):
        self.client.close()

    def get_db(self):
        return self.db
