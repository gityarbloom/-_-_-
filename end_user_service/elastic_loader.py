from moduls import ElasticSearchClient
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


def transform(doc):
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

els = ElasticSearchClient("http://localhost:9200", mapping=map)


# (
#     index="gatekeeper",
#         id=doc["message_id"],
#         document=transform(doc)
#     )