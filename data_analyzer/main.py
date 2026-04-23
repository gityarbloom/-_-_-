from kafka_consumer import KafkaConsumer
from processing_and_analysis import *
import os



print("\n\n🌞 --The DATA-ANALYZERR start his action-- 🌞\n\n")
topic_name = "Gate_Keeper"
consum_conf = {
            'bootstrap.servers': os.getenv("CONSUM_CONFIG"),
            'group.id': 'gatekeeper',
            'auto.offset.reset': 'earliest'
            }

consumer = KafkaConsumer(consum_conf, topic_name)
counter = 0
for msg in consumer.consum():
    print(f"\n❗ A new message (number: {counter}) received\n")
    print(f"\n{msg}\n")
print("\n\n🥱 --The DATA-ANALYZERR finshed his action-- 🥱\n\n")