from confluent_kafka import Consumer, KafkaError



class  KafkaConsumer:
    
    def __init__(self, config:dict, topic_name:str):
        self.config = config
        self.topic_name = topic_name
        self.consumer = None


    def init_consumer(self):
        print("\nTry to connect to kafka⏳...\n")
        try:
            consumer = Consumer(self.config)
            consumer.subscribe([self.topic_name])
            print("\n👍 Kafka consumer is connected!")
            return consumer
        except Exception as e:
            print("\n👎 kafka connection faild")

    def consum(self):
        if not self.consumer:
            self.consumer = self.init_consumer()
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
                massage = msg.value().decode('utf-8')
                print(f"\nReceived message: {massage}\n")
                yield massage
        except Exception as e:
            print(f"\nconsumer failed because: \n{e}\n")
            raise
        finally:
            self.consumer.close()