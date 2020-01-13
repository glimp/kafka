
import pickle
from confluent_kafka.cimpl import Producer

from kafka.consumers.KafkaConsumer import KafkaConsumer
from kafka.consumers.KafkaDelayConsumer import KafkaDelayConsumer
from kafka.consumers.RetryHeader import RetryHeader


class KafkaRetryException(Exception):
    pass


class KafkaRetryConsumer(KafkaConsumer):
    DefaultRetryBackoff = [1, 2, 4]

    class KafkaRetryDelayConsumer(KafkaDelayConsumer):
        def delay_fn(self, fn):
            return fn

    def __init__(self, config, retry_backoff=None):
        super().__init__(config=config)
        if retry_backoff is None:
            retry_backoff = KafkaRetryConsumer.DefaultRetryBackoff
        self._retry_consumers = {}
        self.topics = []
        self.config = config
        self._retry_producer = Producer(config)
        for idx, backoff in enumerate(retry_backoff):
            self._retry_consumers[idx] = KafkaRetryConsumer.KafkaRetryDelayConsumer(config, delay_sec=backoff)

    def subscribe(self, topics, fn):
        for idx, retry_consumer in self._retry_consumers.items():
            delay_millis = retry_consumer.get_delay_millis()
            retry_topics = list(map(lambda topic: topic+"-retry"+str(idx+1), topics))
            retry_consumer.subscribe(retry_topics, self.retry_fn(fn, delay_millis))

        super().subscribe(topics, self.retry_fn(fn, 0))

    def unsubscribe(self):
        for retry_consumer in self._retry_consumers.values():
            retry_consumer.unsubscribe()
        super().unsubscribe()

    def retry_fn(self, fn, delay_millis):
        def wrapped(record):
            retry_header = self.get_retry_header(record) or \
                           RetryHeader(org_topic=record.topic(), org_timestamp=self.get_record_timestamp(record),
                                       retry_cnt=0, retry_timestamp=self.get_record_timestamp(record))
            millis_diff = self.get_record_timestamp(record) - retry_header.retry_timestamp
            try:
                if millis_diff >= delay_millis:
                    fn(record)
            except KafkaRetryException:
                if retry_header.retry_cnt < len(self._retry_consumers):
                    retry_header.retry_cnt += 1
                    retry_header.retry_timestamp = self.get_record_timestamp(record)
                    topic = retry_header.org_topic+"-retry"+str(retry_header.retry_cnt)
                    headers = self.set_retry_header(record, retry_header)
                    self._retry_producer.produce(topic=topic, value=record.value(), key=record.key(), headers=headers)
            return delay_millis - millis_diff
        return wrapped

    def get_retry_header(self, record):
        value = self.get_headers(record).get(RetryHeader.RetryHeaderKey, None)
        return value and pickle.loads(value) or None

    def set_retry_header(self, record, retry_header):
        assert isinstance(retry_header, RetryHeader)
        return self.set_headers(record, RetryHeader.RetryHeaderKey, retry_header)
