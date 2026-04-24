from pymongo import MongoClient
import time


class MongoConnection:
    def __init__(self, mongo_uri:str):
        self.client_conn = self.get_mongodb_client(mongo_uri)
        self.db_name = None


    def get_mongodb_client(self):
        for retry in range(5):
            try:
                time.sleep(2)
                print("\nTry to connect to MongoDB⏳...")
                client_conn = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=2000)
                client_conn.admin.command("ping")
                print(f"\n👍 Cnnected!")
                return client_conn
            except Exception as e:
                print(f"\n👎 Attempt {retry+1} failed: {e}")
                if retry == 4:
                    raise