from queue import Queue
from unittest import TestCase

from confluent_kafka.cimpl import Producer

from kafka.consumers.KafkaConfig import KafkaConfig

import random

from kafka.consumers.KafkaRetryConsumer import KafkaRetryConsumer, KafkaRetryException
import time


class TestKafkaRetryConsumer(TestCase):

    def test_subscribe(self):
        test_retry_backoff = [3, 13]
        queue = Queue()
        config = KafkaConfig(bootstrap_servers='127.0.0.1:9093', group_id='testConsumer%s' % (random.randint), auto_offset_reset='largest')
        producer = Producer(config)
        consumer = KafkaRetryConsumer(config, retry_backoff=test_retry_backoff)

        print("start!!")
        def task(record):
            print("[%20s] subscribe! %s" % (record.topic(), record.value()))
            queue.put( (record.topic(), int(record.value())+1, time.time()) )
            raise KafkaRetryException

        consumer.subscribe(['test'], task)
        try:
            print("here!!")
            time.sleep(10)

            producer.produce('test', str(30))
            produce_time = time.time()
            print("%d produce!" % (produce_time))
            time.sleep(2)
            producer.produce('test', str(50))
            produce_time2 = time.time()
            print("%d produce2!" % (produce_time))

            for i in range(6):
                (topic, value, consume_time) = queue.get()
                if value is 31:
                    print("30 get message %d", consume_time - produce_time)
                else:
                    print("50 get message %d", consume_time - produce_time2)

        finally:
            consumer.unsubscribe()
        print("end")
