from unittest import TestCase

from kafka.consumers.KafkaConfig import KafkaConfig
from kafka.consumers.KafkaDelayConsumer import KafkaDelayConsumer

import time
import random

from kafka.core.KafkaRetryConsumer import KafkaRetryConsumer, KafkaRetryException


class TestKafkaConsumer(TestCase):

    config = KafkaConfig(bootstrap_servers='127.0.0.1:9093', group_id='testConsumer%s' % (random.randint), auto_offset_reset='earliest')


    def test_subscribe(self):
        consumer = KafkaDelayConsumer(self.config, delay_sec=2)
        print("start")

        def task(record):
            print("%s subscribe! %s" % (record.topic(), record.value()))
            #raise KafkaRetryException

        consumer.subscribe(['test'], task)
        print("here!!")
        time.sleep(60)
        consumer.unsubscribe()
        print("end")
