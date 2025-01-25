from time import time, time_ns

from couchbase.exceptions import (
    DocumentExistsException,
    DocumentLockedException,
    DocumentNotFoundException,
)


class Store:
    def __init__(self):
        self._documents = {}

    def insert(self, collection, key, value, expiry):
        if self.exists(collection, key):
            raise DocumentExistsException

        self._add(collection, key, value, expiry)
        return True

    def upsert(self, collection, key, value, expiry):
        self._add(collection, key, value, expiry)
        return True

    def replace(self, collection, key, value, expiry):
        self._add(collection, key, value, expiry)
        return True

    def get(self, collection, key):
        return self._get(collection, key)

    def remove(self, collection, key):
        self._del(collection, key)
        return True

    def exists(self, collection, key):
        try:
            self._get(collection, key)
            return True
        except DocumentNotFoundException:
            return False

    def touch(self, collection, key, expiry):
        document = self._get(collection, key)
        self._add(collection, key, document["value"], expiry)
        return True

    def lock(self, collection, key, time_):
        document = self._get(collection, key)
        self._add(
            collection, key, document["value"], document["expiry"], int(time()) + time_
        )
        return True

    def unlock(self, collection, key):
        document = self._get(collection, key)
        self._add(collection, key, document["value"], document["expiry"])
        return True

    def _add(self, collection, key, value, expiry, lock=0):
        if collection not in self._documents:
            self._documents[collection] = {}

        self._documents[collection][key] = {
            "value": value,
            "expiry": expiry,
            "locked": lock,
            "exists": True,
        }

    def _get(self, collection, key):
        if collection not in self._documents or key not in self._documents[collection]:
            raise DocumentNotFoundException

        document = self._documents[collection][key]
        try:
            if 0 < document["expiry"] < time():
                raise DocumentNotFoundException

            if 0 < document["locked"] < time():
                raise DocumentLockedException

            return {
                "cas": int(time_ns()),
                "key": key,
                "flags": 33554438,
                "value": document["value"],
            }
        except KeyError:
            raise DocumentNotFoundException

    def _del(self, collection, key):
        if collection not in self._documents or key not in self._documents[collection]:
            raise DocumentNotFoundException

        del self._documents[collection][key]
