from moduls import ElasticSearchClient
from moduls import MongoLoader
import time
import os



def get_map():
    return {
        "mappings": {
        "properties": {
        "message_id": { "type": "keyword" },
        "suspect_id": { "type": "keyword" },
        "text": { "type": "text" },
        "date": { "type": "date" },
        "tags": { "type": "keyword" },
        "current_transaction_value": { "type": "float" },
        "initial_risk": { "type": "integer" },
        "credit_rating_factor": { "type": "integer" },
        "final_score": { "type": "float" }
        }
        }
        }


def transform(doc:dict):
    return {
        "message_id": doc["message_id"],
        "suspect_id": doc["suspect_id"],
        "text": doc["text"],
        "date": doc["date"],
        "tags": doc["tags"],
        "current_transaction_value": doc["current_transaction_value"],
        "initial_risk": doc["initial_risk"],
        "credit_rating_factor": doc["credit_rating_factor"],
        "final_score": doc["final_score"]
    }


print("\n\n\n🌞 End-user service START!! 🌞\n\n\n")
time.sleep(5)

mongo = MongoLoader(os.getenv("MONGO_URI", "mongodb://admin:israelyarbloom@localhost:27017/?authSource=admin"))
es = ElasticSearchClient("http://localhost:9200", "gate_keeper", mapping=get_map())
counter = 0

for doc in mongo.get_all_docs("gatekeeper_db", "gatekeeper_coll"):
    counter += 1
    print(f"\n\n****Received DOC number {counter}****")
    print(f"DOC: \n{doc}\n")
    es_doc = transform(doc)

    response = es.index_doc(es_doc["message_id"], es_doc)
    print("\nreceive document in ES index 'gate_keeper'")
    print(f"doc: \n{response["result"]}\n"+"*"*75+"\n")
    time.sleep(1)


time.sleep(5)
print("\n\n\n🥱 End-user service FINSHED!! 🥱\n\n\n")