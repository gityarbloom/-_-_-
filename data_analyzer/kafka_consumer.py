from confluent_kafka import Consumer, KafkaError
import time
import json


class  KafkaConsumer:
    
    def __init__(self, config:dict, topic_name:str):
        self.config = config
        self.topic_name = topic_name
        self.consumer = None


    def init_consumer(self):
        for i in range(5):
            time.sleep(1)
            print("\nTry to connect to kafka⏳...\n")
            try:
                consumer = Consumer(self.config)
                print("\n👍 Kafka consumer is connected!")
                return consumer
            except Exception as e:
                if i == 4:
                    print(f"\n👎 kafka connection faild \n{e}\n")
                    raise

    def consum(self):
        if not self.consumer:
            self.consumer = self.init_consumer()
            self.consumer.subscribe([self.topic_name])
        print("\nWaiting for messages from KRaft cluster...\n")
        try:
            while True:
                msg = self.consumer.poll(2)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"\nError: {msg.error()}\n")
                        break
                yield json.loads(msg.value().decode('utf-8'))
                time.sleep(1)
        except Exception as e:
            print(f"\nconsumer failed because: \n{e}\n")
            raise
        finally:
            self.consumer.close()