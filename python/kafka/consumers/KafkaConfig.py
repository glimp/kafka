
import types
import logging


class KafkaConfig(dict):
    '''

    auto_offset_reset='largest'
        https://kafka.apache.org/documentation/#theconsumer
        The Kafka consumer works by issuing "fetch" requests to the brokers leading the partitions it wants to consume.
        The consumer specifies its offset in the log with each request and receives back a chunk of log beginning
        from that position.
        The consumer thus has significant control over this position and can rewind it to re-consume data if need be.

    '''

    def __init__(self, bootstrap_servers, group_id=None, auto_offset_reset='largest', timeout=None, error_cb=None,
                 throttle_cb=None, stats_cb=None, on_delivery=None, on_commit=None, on_assign=None, on_revoke=None,
                 logger=None):
        super().__init__()
        if not isinstance(bootstrap_servers, str): raise (ValueError, 'bootstrap')
        self['bootstrap.servers'] = bootstrap_servers

        if group_id: self.group_id=group_id
        if auto_offset_reset: self.auto_offset_reset = auto_offset_reset
        if timeout: self.timeout = timeout
        if error_cb: self.error_cb = error_cb
        if throttle_cb: self.throttle_cb = throttle_cb
        if stats_cb: self.stats_cb = stats_cb
        if on_delivery: self.on_delivery = on_delivery
        if on_commit: self.on_commit = on_commit
        if on_assign: self.on_assign = on_assign
        if on_revoke: self.on_revoke = on_revoke
        if logger: self.logger = logger

    @property
    def bootstrap_servers(self):
        return self['bootstrap.servers']

    @property
    def group_id(self):
        return self['group.id']

    @property
    def auto_offset_reset(self):
        return self['auto.offset.reset']

    @property
    def timeout(self):
        return self['timeout']

    @property
    def error_cb(self):
        return self['error_cb']

    @property
    def throttle_cb(self):
        return self['throttle_cb']

    @property
    def stats_cb(self):
        return self['stats_cb']

    @property
    def on_delivery(self):
        return self['on_delivery']

    @property
    def on_commit(self):
        return self['on_commit']

    @property
    def on_assign(self):
        return self['on_assign']

    @property
    def on_revoke(self):
        return self['on_revoke']

    @property
    def logger(self):
        return self['logger']

    @group_id.setter
    def group_id(self, group_id):
        if group_id and not isinstance(group_id, str): raise (ValueError, 'group_id (Consumer)')
        self['group.id'] = group_id

    @auto_offset_reset.setter
    def auto_offset_reset(self, auto_offset_reset):
        if auto_offset_reset and not isinstance(auto_offset_reset, str): raise (ValueError, 'auto_offset_reset')
        self['auto.offset.reset'] = auto_offset_reset

    @timeout.setter
    def timeout(self, timeout):
        if timeout and not isinstance(timeout, float): raise (ValueError, 'timeout')
        self['timeout'] = timeout

    @error_cb.setter
    def error_cb(self, error_cb):
        if error_cb and not isinstance(error_cb, types.LambdaType): raise (
            ValueError, 'error_cb(kafka.KafkaError): Callback for generic/global error events.')
        self['error_cb'] = error_cb

    @throttle_cb.setter
    def throttle_cb(self, throttle_cb):
        if throttle_cb and not isinstance(throttle_cb, types.LambdaType): raise (
            ValueError, 'throttle_cb(confluent_kafka.ThrottleEvent): Callback for throttled request reporting.')
        self['throttle_cb'] = throttle_cb

    @stats_cb.setter
    def stats_cb(self, stats_cb):
        if stats_cb and not isinstance(stats_cb, types.LambdaType): raise (
            ValueError, 'stats_cb(json_str): Callback for statistics data.')
        self['stats_cb'] = stats_cb

    @on_delivery.setter
    def on_delivery(self, on_delivery):
        if on_delivery and not isinstance(on_delivery, types.LambdaType): raise (
            ValueError, 'on_delivery(kafka.KafkaError, kafka.Message) (Producer): \
            value is a Python function reference that is called once for each produced message \
            to indicate the final delivery result (success or failure).')
        self['on_delivery'] = on_delivery

    @on_commit.setter
    def on_commit(self, on_commit):
        if on_commit and not isinstance(on_commit, types.LambdaType): raise (
            ValueError, 'on_delivery(kafka.KafkaError, kafka.Message) (Producer): \
            value is a Python function reference that is called once for each produced message \
            to indicate the final delivery result (success or failure).')
        self['on_commit'] = on_commit

    @on_assign.setter
    def on_assign(self, on_assign):
        if on_assign and not isinstance(on_assign, types.LambdaType): raise (
            ValueError, 'on_assign(consumer, partitions) (Consumer): \
            callback to provide handling of customized offsets on completion of a successful partition re-assignment.')
        self['on_assign'] = on_assign

    @on_revoke.setter
    def on_revoke(self, on_revoke):
        if on_revoke and not isinstance(on_revoke, types.LambdaType): raise (
            ValueError, 'on_revoke(consumer, partitions) (Consumer): \
            callback to provide handling of offset commits to a customized store on the start of a rebalance operation.')
        self['on_revoke'] = on_revoke

    @logger.setter
    def logger(self, logger):
        if logger and not isinstance(logger, logging.Handler): raise (
            ValueError, 'logger=logging.Handler kwarg: forward logs from the Kafka client to the provided logging.')
        self['logger'] = logger
