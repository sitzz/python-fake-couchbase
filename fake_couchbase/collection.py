from datetime import timedelta
from time import time_ns

from couchbase.collection import GetOptions, TouchOptions
from couchbase.logic.collection import CollectionLogic
from couchbase.pycbc_core import result
from couchbase.result import (
    ExistsResult,
    GetResult,
    MultiGetResult,
    MultiMutationResult,
    MutationResult,
)

from fake_couchbase._datetime_hack import utcnow
from fake_couchbase.store import Store

_STORE = Store()


class Collection(CollectionLogic):
    def __init__(self, scope, name):
        super().__init__(scope, name)
        self._scope = scope
        self._collection_name = name
        self._store_name = f"{scope.bucket_name}-{scope.name}-{name}"

    def get(self, key, *opts, **kwargs):
        res = result()
        res.raw_result = _STORE.get(self._store_name, key)
        return GetResult(res)

    def get_multi(self, keys, *opts, **kwargs):
        return_exceptions = self._get_kwarg("return_exceptions", kwargs, True)
        res = result()
        docs = {}
        for key in keys:
            tmp_res = result()
            tmp_res.raw_result = _STORE.get(self._store_name, key)
            docs[key] = tmp_res
            del tmp_res

        res.raw_result = docs
        return MultiGetResult(res, return_exceptions=return_exceptions)

    def exists(self, key, *opts, **kwargs):
        res = result()
        res.raw_result = _STORE.get(self._store_name, key)
        return ExistsResult(res)

    def insert(self, key, value, *opts, **kwargs):
        expiry = self._get_expiry(**kwargs)
        res = result()
        try:
            _STORE.insert(self._store_name, key, value, expiry)
            res.raw_result = {"cas": time_ns(), "key": key}
        except Exception as _exc:
            raise _exc

        return MutationResult(res)

    def insert_multi(self, keys_and_docs, *opts, **kwargs):
        expiry = self._get_expiry(**kwargs)
        return_exceptions = self._get_kwarg("return_exceptions", kwargs, True)
        per_key_options = kwargs.get("per_key_options", {})
        all_ok = True
        ops_res = {}
        for key, doc in keys_and_docs.items():
            key_res = result()
            if key in per_key_options:
                doc_expiry = self._get_expiry(**per_key_options[key])
            else:
                doc_expiry = expiry

            try:
                _STORE.insert(self._store_name, key, doc, doc_expiry)
                key_res.raw_result = {"cas": time_ns(), "key": key}
            except Exception as _exc:
                all_ok = False
                key_res.raw_result = _exc

            ops_res[key] = key_res

        ops_res["all_okay"] = all_ok
        res = result()
        res.raw_result = ops_res
        return MultiMutationResult(res, return_exceptions)

    def upsert(self, key, value, *opts, **kwargs):
        expiry = self._get_expiry(**kwargs)
        res = result()
        try:
            _STORE.upsert(self._store_name, key, value, expiry)
            res.raw_result = {"cas": time_ns(), "key": key}
        except Exception as _exc:
            raise _exc

        return MutationResult(res)

    def upsert_multi(self, keys_and_docs, *opts, **kwargs):
        expiry = self._get_expiry(**kwargs)
        return_exceptions = self._get_kwarg("return_exceptions", kwargs, True)
        per_key_options = kwargs.get("per_key_options", {})
        all_ok = True
        ops_res = {}
        for key, doc in keys_and_docs.items():
            key_res = result()
            if key in per_key_options:
                doc_expiry = self._get_expiry(**per_key_options[key])
            else:
                doc_expiry = expiry

            try:
                _STORE.upsert(self._store_name, key, doc, doc_expiry)
                key_res.raw_result = {"cas": time_ns(), "key": key}
            except Exception as _exc:
                all_ok = False
                key_res.raw_result = _exc

            ops_res[key] = key_res

        ops_res["all_okay"] = all_ok
        res = result()
        res.raw_result = ops_res
        return MultiMutationResult(res, return_exceptions)

    def replace(self, key, value, *opts, **kwargs):
        expiry = self._get_expiry(**kwargs)
        res = result()
        try:
            _STORE.replace(self._store_name, key, value, expiry)
            res.raw_result = {"cas": time_ns(), "key": key}
        except Exception as _exc:
            raise _exc

        return MutationResult(res)

    def replace_multi(self, keys_and_docs, *opts, **kwargs):
        expiry = self._get_expiry(**kwargs)
        return_exceptions = self._get_kwarg("return_exceptions", kwargs, True)
        per_key_options = kwargs.get("per_key_options", {})
        all_ok = True
        ops_res = {}
        for key, doc in keys_and_docs.items():
            key_res = result()
            if key in per_key_options:
                doc_expiry = self._get_expiry(**per_key_options[key])
            else:
                doc_expiry = expiry

            try:
                _STORE.replace(self._store_name, key, doc, doc_expiry)
                key_res.raw_result = {"cas": time_ns(), "key": key}
            except Exception as _exc:
                all_ok = False
                key_res.raw_result = _exc

            ops_res[key] = key_res

        ops_res["all_okay"] = all_ok
        res = result()
        res.raw_result = ops_res
        return MultiMutationResult(res, return_exceptions)

    def remove(self, key, *opts, **kwargs):
        res = result()
        try:
            _STORE.remove(self._store_name, key)
        except Exception as _exc:
            raise _exc

        res.raw_result = {"cas": time_ns(), "key": key}
        return MutationResult(res)

    def remove_multi(self, keys, *opts, **kwargs):
        return_exceptions = self._get_kwarg("return_exceptions", kwargs, True)
        all_ok = True
        ops_res = {}
        for key in keys:
            key_res = result()
            try:
                key_kwargs = kwargs.get("per_key_options", {}).get(key, kwargs)
                self.remove(key, *opts, **key_kwargs)
                key_res.raw_result = {"cas": time_ns(), "key": key}
            except Exception as _exc:
                all_ok = False
                key_res.raw_result = _exc

            ops_res[key] = key_res

        ops_res["all_okay"] = all_ok
        res = result()
        res.raw_result = ops_res
        return MultiMutationResult(res, return_exceptions)

    def touch(self, key, *opts, **kwargs):
        expiry = self._get_expiry(**kwargs)
        res = result()
        try:
            _STORE.touch(self._store_name, key, expiry)
            res.raw_result = {"cas": time_ns(), "key": key}
        except Exception as _exc:
            raise _exc

        return MutationResult(res)

    def get_and_touch(self, key, **kwargs):
        try:
            self.touch(key, TouchOptions(), **kwargs)
            return self.get(key, GetOptions(), **kwargs)
        except Exception as _exc:
            raise _exc

    def get_and_lock(self, key, **kwargs):
        lock_time = self._get_kwarg("lock_time", kwargs)
        try:
            res = self.get(key, GetOptions(), **kwargs)
            _STORE.lock(self._store_name, key, lock_time)
            return res
        except Exception as _exc:
            raise _exc

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

    @staticmethod
    def default_name():
        return "_default"

    def _get_expiry(self, **kwargs) -> float:
        expiry_delta = self._get_kwarg("expiry", kwargs)
        if expiry_delta is not None:
            if isinstance(expiry_delta, int):
                expiry_delta = timedelta(seconds=expiry_delta)
            dt_exp = utcnow() + expiry_delta
            return float(dt_exp.timestamp())

        return 0

    @staticmethod
    def _get_kwarg(arg, kwargs, default=None):
        return kwargs.get(arg, default)

    @property
    def store(self):
        return self._store
