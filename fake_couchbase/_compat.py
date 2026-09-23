"""Compatibility layer for the range of Couchbase SDK versions we support (4.1.9 - 4.6.x).

The fake subclasses private ``couchbase.logic`` classes and feeds hand-built data into
the SDK's result classes, so it depends on SDK internals. Every place where those
internals differ between SDK versions is handled here, and nowhere else.

All differences we care about landed in SDK 4.6.0:
    * ``couchbase.logic.{cluster,bucket,scope,collection}.*Logic`` became
      ``couchbase.logic.*_impl.*Impl``, and the bucket/scope/collection constructors
      swapped to ``(name, parent)``.
    * ``ClusterImpl.__init__`` connects to the server, and ``BucketImpl.__init__``
      opens the bucket, unless told not to.
    * ``Result`` classes decode content lazily through a transcoder, so they need
      encoded bytes plus flags rather than an already-decoded value, and they take
      the document key as a constructor argument rather than reading ``raw_result``.
    * ``ExistsResult`` reads ``document_exists`` instead of ``exists``.
"""

from importlib.metadata import PackageNotFoundError, version
from typing import Any, Dict, Tuple

from couchbase.result import (
    ExistsResult,
    GetResult,
    MultiGetResult,
    MultiMutationResult,
    MutationResult,
)
from couchbase.transcoder import JSONTranscoder

from fake_couchbase.result import RawResult


def _sdk_version() -> Tuple[int, ...]:
    # couchbase.__version__ does not exist on older SDKs, so use package metadata.
    try:
        raw = version("couchbase")
    except PackageNotFoundError:
        return ()

    parts = []
    for chunk in raw.split(".")[:3]:
        digits = ""
        for char in chunk:
            if not char.isdigit():
                break
            digits += char
        if not digits:
            break
        parts.append(int(digits))

    return tuple(parts)


SDK_VERSION = _sdk_version()
# An undetectable version is assumed to be the newest API shape.
SDK_46 = not SDK_VERSION or SDK_VERSION >= (4, 6, 0)


if SDK_46:
    from couchbase.logic.cluster_impl import ClusterImpl
    from couchbase.logic.bucket_impl import BucketImpl
    from couchbase.logic.scope_impl import ScopeImpl
    from couchbase.logic.collection_impl import CollectionImpl

    # The 4.6 *Impl classes are composed by the public classes, which hold them in
    # `_impl`, and each constructor reads `parent._impl`. Our fakes *are* the impl,
    # so point `_impl` back at the instance itself.

    class ClusterBase(ClusterImpl):
        def __init__(self, connstr, *options, **kwargs):
            # The SDK's own test hook for building a cluster without a server.
            kwargs.setdefault("skip_connect", "TEST_SKIP_CONNECT")
            super().__init__(connstr, *options, **kwargs)

        @property
        def _impl(self):
            return self

    class BucketBase(BucketImpl):
        def __init__(self, cluster, bucket_name):
            super().__init__(bucket_name, cluster)

        def open_bucket(self, *args, **kwargs):
            pass

        # Public on couchbase.bucket.Bucket, but not provided by BucketImpl.
        @property
        def name(self):
            return self._bucket_name

        @property
        def _impl(self):
            return self

    class ScopeBase(ScopeImpl):
        def __init__(self, bucket, scope_name):
            super().__init__(scope_name, bucket)

        @property
        def _impl(self):
            return self

    class CollectionBase(CollectionImpl):
        def __init__(self, scope, name):
            super().__init__(name, scope)

else:
    # couchbase.logic.scope and couchbase.collection import each other, and the
    # cycle only resolves when couchbase.collection is imported first.
    import couchbase.collection  # noqa: F401
    from couchbase.logic.cluster import ClusterLogic as ClusterBase  # noqa: F401
    from couchbase.logic.bucket import BucketLogic as BucketBase  # noqa: F401
    from couchbase.logic.scope import ScopeLogic as ScopeBase  # noqa: F401
    from couchbase.logic.collection import CollectionLogic as CollectionBase  # noqa: F401


_TRANSCODER = JSONTranscoder()


def _raw_document(document: Dict[str, Any]) -> RawResult:
    """Wrap a document as returned by ``Store.get`` for a ``GetResult``."""
    raw = dict(document)
    if SDK_46:
        raw["value"], raw["flags"] = _TRANSCODER.encode_value(document["value"])

    return RawResult(raw)


def get_result(document: Dict[str, Any]) -> GetResult:
    if SDK_46:
        return GetResult(
            _raw_document(document), transcoder=_TRANSCODER, key=document["key"]
        )

    return GetResult(_raw_document(document))


def exists_result(key: str, cas: int, exists: bool) -> ExistsResult:
    raw = {"cas": cas, "key": key}
    if SDK_46:
        raw["document_exists"] = exists
        return ExistsResult(RawResult(raw), key=key)

    raw["exists"] = exists
    return ExistsResult(RawResult(raw))


def mutation_result(key: str, cas: int) -> MutationResult:
    raw = RawResult({"cas": cas, "key": key})
    if SDK_46:
        return MutationResult(raw, key=key)

    return MutationResult(raw)


def _add_errors(multi_result, errors: Dict[str, Exception], return_exceptions: bool):
    # Exceptions are attached after construction rather than passed in: 4.6 accepts
    # Python exceptions in raw_result, but older SDKs only recognise the C-level ones.
    if errors and not return_exceptions:
        raise next(iter(errors.values()))

    multi_result._results.update(errors)
    return multi_result


def multi_get_result(
    documents: Dict[str, Dict[str, Any]],
    errors: Dict[str, Exception],
    return_exceptions: bool,
) -> MultiGetResult:
    raw = {key: _raw_document(document) for key, document in documents.items()}
    raw["all_okay"] = not errors
    if SDK_46:
        transcoders = {key: _TRANSCODER for key in documents}
        res = MultiGetResult(RawResult(raw), return_exceptions, transcoders)
        # 4.6 takes the key as a constructor argument, which MultiResult does not pass on.
        for key, doc_res in res._results.items():
            doc_res._key = key
    else:
        res = MultiGetResult(RawResult(raw), return_exceptions)

    return _add_errors(res, errors, return_exceptions)


def multi_mutation_result(
    cas_values: Dict[str, int],
    errors: Dict[str, Exception],
    return_exceptions: bool,
) -> MultiMutationResult:
    raw = {key: RawResult({"cas": cas, "key": key}) for key, cas in cas_values.items()}
    raw["all_okay"] = not errors
    res = MultiMutationResult(RawResult(raw), return_exceptions)
    return _add_errors(res, errors, return_exceptions)
