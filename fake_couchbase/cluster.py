from datetime import timedelta
from time import sleep, time
from random import randint

from couchbase.exceptions import UnAmbiguousTimeoutException
from couchbase.logic.cluster import ClusterLogic
from couchbase.serializer import DefaultJsonSerializer
from couchbase.transcoder import JSONTranscoder

from fake_couchbase._datetime_hack import utcnow
from fake_couchbase.bucket import Bucket


class Cluster(ClusterLogic):
    def __init__(self, connstr, *options, **kwargs):
        super().__init__(connstr, *options, **kwargs)
        self._connected = False
        self._default_serializer = DefaultJsonSerializer()
        self._transcoder = JSONTranscoder()
        self._connect()

    def close(self):
        self._connected = False

    def bucket(self, bucket_name):
        return Bucket(self, bucket_name)

    def cluster_info(self):
        pass

    def ping(self, *opts, **kwargs):
        pass

    def diagnostics(self, *opts, **kwargs):
        pass

    @staticmethod
    def wait_until_ready(timeout: timedelta, *opts, **kwargs):
        cutoff = (utcnow() + timeout).timestamp()
        wait = 1 / randint(100, 500)
        if time() + wait < cutoff:
            sleep(wait)
            return

        sleep(timeout.seconds)
        raise UnAmbiguousTimeoutException

    def query(self, statement, *options, **kwargs):
        pass

    def analytics_query(self, statement, *options, **kwargs):
        pass

    def search_query(self, index, query, *options, **kwargs):
        pass

    def search(self, index, request, *options, **kwargs):
        pass

    def buckets(self):
        pass

    def users(self):
        pass

    def query_indexes(self):
        pass

    def analytics_indexes(self):
        pass

    def search_indexes(self):
        pass

    def eventing_functions(self):
        pass

    def _connect(self):
        self._connected = True

    @property
    def connected(self):
        return self._connected
