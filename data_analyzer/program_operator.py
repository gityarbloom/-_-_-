from mongo_loader import MongoLoader
from mysql_analyzer import MySqlConnection
from kafka_consumer import KafkaConsumer
from processing_and_analysis import *



def play(config):
    print("\n\n🌞 --The DATA-ANALYZERR start his action-- 🌞\n\n")

    editor = TextEditor()
    consumer = KafkaConsumer(config.kafka, "Gate_Keeper")
    sql_db = MySqlConnection(**config.mysql)
    mongo_db = MongoLoader(config.mongo)
    counter = 0

    for msg in consumer.consum():
        if not len(msg) == 5:
            continue
        counter +=1
        print(f"\nReceived message: {msg}")
        print(f"\n***{counter}***")
        msg["text"] = editor.clean_html(msg["text"])
        msg["current_transaction_value"] = editor.price_extraction(msg["text"])
        risks = sql_db.get_risks(msg["suspect_id"])
        initial_risk = risks[0]
        credit_rating_factor = risks[1]
        if initial_risk > 7:
            credit_rating_factor = credit_rating_factor *1.5
        final_score = initial_risk +(msg["current_transaction_value"]/1000) *credit_rating_factor
        msg["initial_risk"] = initial_risk
        msg["credit_rating_factor"] = credit_rating_factor
        msg["final_score"] = final_score
        mongo_db.insert("gatekeeper_db", "gatekeeper_coll", [msg])