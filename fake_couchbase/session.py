from datetime import timedelta
import logging
from typing import Any, Dict

from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions, ClusterTimeoutOptions

from fake_couchbase.exceptions import BucketNotSet, ScopeNotSet
from fake_couchbase.protocols import SessionProt
from fake_couchbase.cluster import Cluster
from fake_couchbase.timeout import Timeout


class Session(SessionProt):
    def __init__(self, hostname: str, username: str, password: str, **kwargs):
        # Initiate logger
        self.logger = kwargs.get("logger", logging.getLogger())

        # Set default cluster values
        self._hostname = hostname
        self._username = username
        self._password = password
        self._cluster = None
        self._bucket = None
        self._bucket_name = kwargs.get("bucket", "test")
        self._scope = None
        self._scope_name = kwargs.get("scope", "_default")
        self._collection = None
        self._collection_name = kwargs.get("collection", "_default")
        self._connected = False
        self._tls = kwargs.get("tls", False)

        # Set the session's timeouts
        timeout = kwargs.get("timeout", Timeout())
        if isinstance(timeout, Timeout):
            self._timeout = timeout
        elif isinstance(timeout, tuple):
            self._timeout = Timeout(*timeout)
        elif isinstance(timeout, int):
            self._timeout = Timeout(timeout, timeout, timeout)
        elif timeout is None:
            self._timeout = Timeout()

        timeout_options = ClusterTimeoutOptions(
            connect_timeout=timedelta(seconds=self._timeout.connection)
        )
        self.options = ClusterOptions(
            authenticator=PasswordAuthenticator(
                username=self._username,
                password=self._password,
            ),
            enable_tls=self._tls,
            timeout_options=timeout_options,
            enable_tracing=True,
            show_queries=True,
        )

        self._documents: Dict[str, Any] = {}

    @property
    def connection_string(self) -> str:
        """Generates the cluster's connection string

        Returns:
            str: A combination of the protocol and hostname for the connection
        """
        return f"fakecouchbase{'s' if self._tls else ''}://{self._hostname}"

    def connect(self):
        self._cluster = Cluster(
            self.connection_string,
            self.options,
        )

        self.logger.debug("- Connecting to cluster: %s", self._hostname)

        if self._bucket_name is not None:
            self.bucket = self._bucket_name

        if self._scope_name is not None:
            self.scope = self._scope_name

        if self._collection_name is not None:
            self.collection = self._collection_name

        self._connected = True

    def disconnect(self):
        self._connected = False

    @property
    def connected(self):
        return self._connected

    @property
    def cluster(self):
        return self._cluster

    @property
    def bucket(self):
        return self._bucket

    @bucket.setter
    def bucket(self, value):
        self._bucket = self._cluster.bucket(value)
        self._bucket_name = value

    @property
    def bucket_name(self):
        return self._bucket_name

    @property
    def scope(self):
        return self._scope

    @scope.setter
    def scope(self, value):
        if self._bucket is None:
            raise BucketNotSet("no bucket set")

        self._scope = self._bucket.scope(value)
        self._scope_name = value

    @property
    def collection(self):
        return self._collection

    @collection.setter
    def collection(self, value):
        if self._scope is None:
            raise ScopeNotSet("no scope set")

        self._collection = self._scope.collection(value)
        self._collection_name = value

    def ping(self):
        return self._connected

    @property
    def timeout(self) -> Timeout:
        return self._timeout
