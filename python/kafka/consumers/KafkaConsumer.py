import functools
import pickle
import types
from concurrent.futures.thread import ThreadPoolExecutor

from confluent_kafka import Consumer
import threading

from confluent_kafka.cimpl import TIMESTAMP_NOT_AVAILABLE


class KafkaConsumer(object):

    def __init__(self, config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop = threading.Event()
        self._running = False
        self._pool = ThreadPoolExecutor(1)
        self.timeout = config.pop('timeout', 1.0)
        self.num_messages = config.pop('num_messages', 1000)
        self.on_assign = config.pop('on_assign', None)
        self.on_revoke = config.pop('on_revoke', None)
        self._consumer = Consumer(config)

    def subscribe(self, topics, fn):
        copied_fn = functools.update_wrapper(
            types.FunctionType(
                fn.__code__,
                fn.__globals__,
                name=fn.__name__,
                argdefs=fn.__defaults__,
                closure=fn.__closure__
            ), fn)
        copied_fn.__kwdefaults__ = fn.__kwdefaults__

        if self._running:
            raise RuntimeError
        self._running = True

        topics = isinstance(topics, str) and [topics] or topics

        if self.on_assign and self.on_revoke:
            self._consumer.subscribe(topics=topics, on_assign=self.on_assign, on_revoke=self.on_revoke)
        elif self.on_assign:
            self._consumer.subscribe(topics=topics, on_assign=self.on_assign)
        elif self.on_revoke:
            self._consumer.subscribe(topics=topics, on_revoke=self.on_revoke)
        else:
            self._consumer.subscribe(topics=topics)

        self._pool.submit(self.consumer_task, copied_fn)

    def consumer_task(self, fn):
        try:
            while not self._stop.is_set():
                records = self._consumer.consume(num_messages=self.num_messages, timeout=self.timeout)
                if records:
                    self.on_records(records, fn)
        finally:
            self._consumer.close()

    def unsubscribe(self):
        if self._running:
            self._consumer.unsubscribe()
            self._stop.set()
            self._pool.shutdown()

    def on_records(self, records, fn):
        for record in records:
            return fn(record)

    def commit(self):
        self._consumer.commit(asynchronous=False)

    def get_record_timestamp(self, record):
        (timestamp_type, timestamp) = record.timestamp()
        assert timestamp_type is not TIMESTAMP_NOT_AVAILABLE
        return timestamp

    def get_headers(self, record):
        return dict((k, v) for k, v in record.headers()) if record.headers() else dict()

    def set_headers(self, record, key, value):
        headers = self.get_headers(record)
        headers[key] = pickle.dumps(value)
        record.set_headers(list(headers.items()))
        return list(headers.items())
