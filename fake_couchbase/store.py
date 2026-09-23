from datetime import timedelta
from time import time, time_ns

from couchbase.exceptions import (
    DocumentExistsException,
    DocumentLockedException,
    DocumentNotFoundException,
)


class Store:
    def __init__(self):
        self._documents = {}
        self._indexes = {}
        self._indexed_documents = {}

    def insert(self, collection, key, value, expiry):
        if self.exists(collection, key):
            raise DocumentExistsException

        self._add(collection, key, value, expiry)
        return True

    def upsert(self, collection, key, value, expiry):
        if self.exists(collection, key):
            self._check_unlocked(collection, key)

        self._add(collection, key, value, expiry)
        return True

    def replace(self, collection, key, value, expiry):
        self._check_unlocked(collection, key)
        self._add(collection, key, value, expiry)
        return True

    def get(self, collection, key):
        return self._get(collection, key)

    def remove(self, collection, key):
        self._check_unlocked(collection, key)
        self._del(collection, key)
        return True

    def exists(self, collection, key):
        try:
            self._get(collection, key)
            return True
        except DocumentNotFoundException:
            return False

    def touch(self, collection, key, expiry):
        self._check_unlocked(collection, key)
        document = self._get(collection, key)
        self._add(collection, key, document["value"], expiry)
        return True

    def lock(self, collection, key, time_):
        self._check_unlocked(collection, key)
        if isinstance(time_, timedelta):
            time_ = time_.total_seconds()

        document = self._record(collection, key)
        self._add(collection, key, document["value"], document["expiry"], time() + time_)
        return True

    def unlock(self, collection, key):
        document = self._record(collection, key)
        self._add(collection, key, document["value"], document["expiry"])
        return True

    def add_index(self, collection, name, columns):
        self._indexes[collection][name] = columns

        if collection not in self._indexed_documents:
            self._indexed_documents[collection] = {}
        if name not in self._indexed_documents[collection]:
            self._indexed_documents[collection][name] = set()

        for key, doc in self._documents[collection].items():
            if not all([col in doc for col in columns]):
                continue

            self._indexed_documents[collection][name].add(key)

    def _add(self, collection, key, value, expiry, lock=0):
        if collection not in self._documents:
            self._documents[collection] = {}

        self._documents[collection][key] = {
            "value": value,
            "expiry": expiry,
            "locked": lock,
            "exists": True,
        }

        if collection not in self._indexes:
            return

        for index, columns in self._indexes[collection].items():
            if not all([col in value for col in columns]):
                continue

            self._indexed_documents[collection][index].add(key)

    def _record(self, collection, key):
        """Return the stored record for a live document, raising if there is none."""
        if collection not in self._documents or key not in self._documents[collection]:
            raise DocumentNotFoundException

        document = self._documents[collection][key]
        if 0 < document["expiry"] < time():
            raise DocumentNotFoundException

        return document

    def _check_unlocked(self, collection, key):
        # As on a real server, a lock only blocks writes; reads still succeed.
        if time() < self._record(collection, key)["locked"]:
            raise DocumentLockedException

    def _get(self, collection, key):
        document = self._record(collection, key)
        return {
            "cas": int(time_ns()),
            "key": key,
            "flags": 33554438,
            "value": document["value"],
        }

    def _del(self, collection, key):
        if collection not in self._documents or key not in self._documents[collection]:
            raise DocumentNotFoundException

        del self._documents[collection][key]

        if collection not in self._indexes:
            return

        for index in self._indexes[collection].keys():
            self._indexed_documents[collection][index].discard(key)
