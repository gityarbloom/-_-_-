from pymongo import MongoClient
from pymongo.errors import BulkWriteError, PyMongoError
import time


class MongoLoader:
    def __init__(self, mongo_uri:str):
        self.mongo_uri = mongo_uri
        self.client_conn = self.get_mongodb_client()


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


    
    def insert(self, db:str, coll:str, docs:list[dict]):
        loader = self.client_conn[db][coll]
        if len(docs) > 1:
            print(f"\n\nInsert A batch of documents to MongoDB")
            try:
                result = loader.insert_many(documents=docs)
                print(f"\nInserted {len(result.inserted_ids)} docs\n")
            except BulkWriteError as e:
                print("\nBulk write error:\n")
                for err in e.details.get("\nwriteErrors", []):
                    print(f"\nFailed at index {err['index']}: {err['errmsg']}\n")

        print(f"\n\nInsert one doc to MongoDB")
        try:
            result = loader.insert_one(docs[0])
            print("\nInserted id:", result.inserted_id)
        except PyMongoError as e:
            print("\nMongo error:", e)
        