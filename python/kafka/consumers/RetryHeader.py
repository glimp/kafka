
import time


class RetryHeader(dict):
    RetryHeaderKey = "RetryHeader"

    def __init__(self, org_topic, org_timestamp=time.time() * 1000, retry_cnt=0, retry_timestamp=time.time() * 1000):
        super().__init__()
        self["retry_cnt"] = retry_cnt
        self["retry_timestamp"] = retry_timestamp
        self["org_topic"] = org_topic
        self["org_timestamp"] = org_timestamp

    @property
    def retry_cnt(self): return self["retry_cnt"]

    @property
    def retry_timestamp(self): return self["retry_timestamp"]

    @property
    def org_topic(self): return self["org_topic"]

    @property
    def org_timestamp(self): return self["org_timestamp"]

    @retry_cnt.setter
    def retry_cnt(self, retry_cnt): self['retry_cnt'] = retry_cnt

    @retry_timestamp.setter
    def retry_timestamp(self, retry_timestamp): self['retry_timestamp'] = retry_timestamp

    @org_topic.setter
    def org_topic(self, org_topic): self['org_topic'] = org_topic

    @org_timestamp.setter
    def org_timestamp(self, org_timestamp): self['org_timestamp'] = org_timestamp
