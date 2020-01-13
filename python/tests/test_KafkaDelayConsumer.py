from queue import Queue
from unittest import TestCase

from confluent_kafka.cimpl import Producer

from kafka.consumers.KafkaConfig import KafkaConfig
from kafka.consumers.KafkaDelayConsumer import KafkaDelayConsumer

import time
import random


class TestKafkaDelayConsumer(TestCase):

    config = KafkaConfig(bootstrap_servers='127.0.0.1:9093', group_id='testConsumer%s' % (random.randint), auto_offset_reset='latest')


    def test_subscribe(self):
        queue = Queue()
        test_delay_sec = 15
        producer = Producer(self.config)
        consumer = KafkaDelayConsumer(self.config, delay_sec=test_delay_sec)

        print("start!!")
        def task(record):
            print("%s subscribe! %s" % (record.topic(), record.value()))
            queue.put( (record.topic(), int(record.value())+1, time.time()) )

        consumer.subscribe(['test'], task)

        time.sleep(3)
        producer.produce('test', str(20))
        produce_time = time.time()
        print("%d produce!" % (produce_time))
        time.sleep(9)
        producer.produce('test', str(30))
        produce_time2 = time.time()
        print("%d produce!" % (time.time()))

        (topic, value, consume_time) = queue.get()
        print("get message %d", consume_time - produce_time, consume_time)
        #self.assertTrue(test_delay_sec <= int(consume_time - produce_time) <= test_delay_sec+1)

        (topic, value, consume_time) = queue.get()
        print("get message2 %d", consume_time - produce_time2, consume_time)
        consumer.unsubscribe()
        print("end")
