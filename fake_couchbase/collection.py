from datetime import timedelta
from time import time, time_ns

from couchbase.exceptions import DocumentNotFoundException
from couchbase.options import GetOptions, TouchOptions

from fake_couchbase._compat import (
    CollectionBase,
    exists_result,
    get_result,
    multi_get_result,
    multi_mutation_result,
    mutation_result,
)
from fake_couchbase.store import Store

_STORE = Store()


class Collection(CollectionBase):
    def __init__(self, scope, name):
        super().__init__(scope, name)
        self._scope = scope
        self._collection_name = name
        self._store_name = f"{scope.bucket_name}-{scope.name}-{name}"

    def get(self, key, *opts, **kwargs):
        return get_result(_STORE.get(self._store_name, key))

    def get_multi(self, keys, *opts, **kwargs):
        options = self._options(opts, kwargs)
        return_exceptions = options.get("return_exceptions", True)
        docs = {}
        errors = {}
        for key in keys:
            try:
                docs[key] = _STORE.get(self._store_name, key)
            except DocumentNotFoundException as _exc:
                errors[key] = _exc

        return multi_get_result(docs, errors, return_exceptions)

    def exists(self, key, *opts, **kwargs):
        return exists_result(key, time_ns(), _STORE.exists(self._store_name, key))

    def insert(self, key, value, *opts, **kwargs):
        expiry = self._get_expiry(*opts, **kwargs)
        _STORE.insert(self._store_name, key, value, expiry)
        return mutation_result(key, time_ns())

    def insert_multi(self, keys_and_docs, *opts, **kwargs):
        return self._mutate_multi(_STORE.insert, keys_and_docs, *opts, **kwargs)

    def upsert(self, key, value, *opts, **kwargs):
        expiry = self._get_expiry(*opts, **kwargs)
        _STORE.upsert(self._store_name, key, value, expiry)
        return mutation_result(key, time_ns())

    def upsert_multi(self, keys_and_docs, *opts, **kwargs):
        return self._mutate_multi(_STORE.upsert, keys_and_docs, *opts, **kwargs)

    def replace(self, key, value, *opts, **kwargs):
        expiry = self._get_expiry(*opts, **kwargs)
        _STORE.replace(self._store_name, key, value, expiry)
        return mutation_result(key, time_ns())

    def replace_multi(self, keys_and_docs, *opts, **kwargs):
        return self._mutate_multi(_STORE.replace, keys_and_docs, *opts, **kwargs)

    def remove(self, key, *opts, **kwargs):
        _STORE.remove(self._store_name, key)
        return mutation_result(key, time_ns())

    def remove_multi(self, keys, *opts, **kwargs):
        options = self._options(opts, kwargs)
        return_exceptions = options.get("return_exceptions", True)
        per_key_options = options.get("per_key_options") or {}
        cas_values = {}
        errors = {}
        for key in keys:
            try:
                self.remove(key, per_key_options.get(key, {}))
                cas_values[key] = time_ns()
            except Exception as _exc:
                errors[key] = _exc

        return multi_mutation_result(cas_values, errors, return_exceptions)

    def touch(self, key, *opts, **kwargs):
        expiry = self._get_expiry(*opts, **kwargs)
        _STORE.touch(self._store_name, key, expiry)
        return mutation_result(key, time_ns())

    def get_and_touch(self, key, **kwargs):
        try:
            self.touch(key, TouchOptions(), **kwargs)
            return self.get(key, GetOptions(), **kwargs)
        except Exception as _exc:
            raise _exc

    def get_and_lock(self, key, lock_time, *opts, **kwargs):
        # Lock first: it raises if the document is missing or already locked.
        _STORE.lock(self._store_name, key, lock_time)
        return self.get(key, GetOptions())

    def unlock(self, key, cas, *opts, **kwargs):
        _STORE.unlock(self._store_name, key)

    def lookup_in(self, key, spec, *opts, **kwargs):
        document = self.get(key, *opts, **kwargs)
        value = document.value
        ret = {}
        for s in spec:
            if s in value:
                ret[s] = value[s]

        return ret

    def _mutate_multi(self, operation, keys_and_docs, *opts, **kwargs):
        options = self._options(opts, kwargs)
        expiry = self._get_expiry(options)
        return_exceptions = options.get("return_exceptions", True)
        per_key_options = options.get("per_key_options") or {}
        cas_values = {}
        errors = {}
        for key, doc in keys_and_docs.items():
            if key in per_key_options:
                doc_expiry = self._get_expiry(per_key_options[key])
            else:
                doc_expiry = expiry

            try:
                operation(self._store_name, key, doc, doc_expiry)
                cas_values[key] = time_ns()
            except Exception as _exc:
                errors[key] = _exc

        return multi_mutation_result(cas_values, errors, return_exceptions)

    @staticmethod
    def default_name():
        return "_default"

    def _get_expiry(self, *opts, **kwargs) -> float:
        expiry_delta = self._options(opts, kwargs).get("expiry")
        # An expiry of zero means the document never expires, as on a real server.
        if expiry_delta:
            if isinstance(expiry_delta, int):
                expiry_delta = timedelta(seconds=expiry_delta)
            # Compared against time() in Store, so it must be plain epoch seconds.
            return time() + expiry_delta.total_seconds()

        return 0

    @staticmethod
    def _options(opts, kwargs):
        """Merge options the way the SDK does: option blocks (``InsertOptions`` and
        friends are dicts) passed positionally or as ``opts=``, then keyword
        arguments, which take precedence."""
        options = {}
        for block in (*opts, kwargs.get("opts")):
            if isinstance(block, dict):
                options.update(block)

        options.update((k, v) for k, v in kwargs.items() if k != "opts")
        return options

    @property
    def store(self):
        return self._store
