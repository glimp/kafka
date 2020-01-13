import pickle
import threading
import time

from confluent_kafka.cimpl import Producer
from kafka.consumers.KafkaConsumer import KafkaConsumer


class KafkaDelayConsumer(KafkaConsumer):

    def __init__(self, config, delay_sec=0):
        super().__init__(config)
        self._paused = threading.Event()
        self._delay_producer = Producer(config)
        self.delay_millis = delay_sec*1000

    def on_records(self, records, fn):
        delay_records = []
        for record in records:
            delayed_time = self.delay_fn(fn)(record) or 0
            if delayed_time > 0:
                delay_records.append((delayed_time, record))

        for _, delay_record in delay_records:
            self._delay_producer.produce(
                topic=delay_record.topic(), value=delay_record.value(), key=delay_record.key(),
                headers=self.get_headers(delay_record))

        if delay_records:
            min_diff = min(delay_records, key=lambda x: x[0])[0]
            if min_diff > self.timeout*1000:
                self.pause(min_diff)

    def delay_fn(self, fn):
        def wrapped(record):
            delay_time = self.get_retry_header(record) or self.get_record_timestamp(record)
            millis_diff = self.get_record_timestamp(record) - delay_time
            if millis_diff >= self.delay_millis:
                return fn(record)
            else:
                self.get_retry_header(record) or self.set_retry_header(record, delay_time)
                return self.delay_millis - millis_diff
        return wrapped

    def get_delay_millis(self):
        return self.delay_millis

    def pause(self, delay_millis):
        def delayed_fn():
            time.sleep(float(delay_millis) / 1000)
            self.resume()

        if not self._paused.is_set():
            self._consumer.pause(self._consumer.assignment())
            self._paused.set()
            threading.Thread(target=delayed_fn).start()
        else:
            self._paused.clear()

    def resume(self):
        if self._paused.is_set():
            self._consumer.resume(self._consumer.assignment())

    def get_retry_header(self, record):
        value = self.get_headers(record).get("DelayHeader", None)
        return value and pickle.loads(value) or None

    def set_retry_header(self, record, value):
        return self.set_headers(record, "DelayHeader", value)
