from moduls import MongoLoader
from fastapi import FastAPI
import time
import os


print("\n\nEnd-user service START!!\n")
time.sleep(10)
mongo = MongoLoader(os.getenv("MONGO_URI", "mongodb://admin:israelyarbloom@localhost:27017/?authSource=admin"))

counter = 0
for doc in mongo.get_all_docs("gatekeeper_db", "gatekeeper_coll"):
    counter += 1
    print(f"\n\n****{counter}****")
    print(f"\n{doc}")
    time.sleep(0.5)