import time
import uuid
from datetime import timedelta

import pytest
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (
    DocumentExistsException,
    DocumentLockedException,
    DocumentNotFoundException,
)
from couchbase.options import (
    ClusterOptions,
    GetMultiOptions,
    UpsertMultiOptions,
    UpsertOptions,
)

import fake_couchbase.collection
import fake_couchbase.store
from fake_couchbase._compat import SDK_46, SDK_VERSION
from fake_couchbase.cluster import Cluster

DOC = {"foo": "bar", "nested": {"list": [1, 2, 3]}}


@pytest.fixture(scope="module")
def cluster():
    options = ClusterOptions(authenticator=PasswordAuthenticator("test", "testtest"))
    return Cluster("couchbase://localhost", options)


@pytest.fixture
def collection(cluster):
    # The store is a module-level singleton, so give every test its own collection.
    return cluster.bucket("test").default_scope().collection(uuid.uuid4().hex)


# Zones on both sides of UTC. Bug: a naive utcnow() read as local time skewed every
# epoch value by the UTC offset, which a UTC-only CI run can never notice.
TIMEZONES = ["UTC", "Europe/Copenhagen", "America/Los_Angeles", "Pacific/Kiritimati"]


@pytest.fixture(params=TIMEZONES)
def local_timezone(request, monkeypatch):
    if not hasattr(time, "tzset"):
        pytest.skip("time.tzset() is not available on this platform")

    monkeypatch.setenv("TZ", request.param)
    time.tzset()
    yield request.param
    monkeypatch.undo()
    time.tzset()


def test_sdk_version_detected():
    assert len(SDK_VERSION) == 3
    assert SDK_46 == (SDK_VERSION >= (4, 6, 0))


def test_cluster_is_connected(cluster):
    assert cluster.connected


def test_names(cluster):
    bucket = cluster.bucket("test")
    scope = bucket.default_scope()
    collection = scope.collection("docs")
    assert bucket.name == "test"
    assert scope.name == "_default"
    assert collection.name == "docs"
    assert bucket.default_collection().name == "_default"


def test_get_returns_decoded_content(collection):
    collection.upsert("key", DOC)
    res = collection.get("key")
    assert res.content_as[dict] == DOC
    assert res.value == DOC
    assert res.key == "key"
    assert res.cas > 0


def test_get_missing_raises(collection):
    with pytest.raises(DocumentNotFoundException):
        collection.get("missing")


def test_exists(collection):
    collection.upsert("key", DOC)
    assert collection.exists("key").exists is True
    assert collection.exists("missing").exists is False


def test_insert(collection):
    res = collection.insert("key", DOC)
    assert res.key == "key"
    assert res.cas > 0
    assert collection.get("key").content_as[dict] == DOC


def test_insert_existing_raises(collection):
    collection.insert("key", DOC)
    with pytest.raises(DocumentExistsException):
        collection.insert("key", DOC)


def test_replace(collection):
    collection.upsert("key", DOC)
    collection.replace("key", {"replaced": True})
    assert collection.get("key").content_as[dict] == {"replaced": True}


def test_remove(collection):
    collection.upsert("key", DOC)
    res = collection.remove("key")
    assert res.key == "key"
    assert collection.exists("key").exists is False


def test_remove_missing_raises(collection):
    with pytest.raises(DocumentNotFoundException):
        collection.remove("missing")


def test_lookup_in(collection):
    collection.upsert("key", DOC)
    assert collection.lookup_in("key", ["foo"]) == {"foo": "bar"}


def test_get_multi(collection):
    docs = {f"key-{i}": {"index": i} for i in range(3)}
    for key, doc in docs.items():
        collection.upsert(key, doc)

    res = collection.get_multi(list(docs))
    assert res.all_ok
    assert {key: r.content_as[dict] for key, r in res.results.items()} == docs
    assert {key: r.key for key, r in res.results.items()} == {k: k for k in docs}


@pytest.mark.parametrize("method", ["insert_multi", "upsert_multi"])
def test_mutate_multi(collection, method):
    docs = {f"key-{i}": {"index": i} for i in range(3)}
    res = getattr(collection, method)(docs)
    assert res.all_ok
    assert set(res.results) == set(docs)
    assert all(r.key == key and r.cas > 0 for key, r in res.results.items())
    for key, doc in docs.items():
        assert collection.get(key).content_as[dict] == doc


def test_replace_multi(collection):
    collection.upsert("key", DOC)
    res = collection.replace_multi({"key": {"replaced": True}})
    assert res.all_ok
    assert collection.get("key").content_as[dict] == {"replaced": True}


def test_insert_multi_partial_failure(collection):
    collection.insert("taken", DOC)
    res = collection.insert_multi({"taken": DOC, "free": DOC})
    assert not res.all_ok
    assert set(res.results) == {"free"}
    assert isinstance(res.exceptions["taken"], DocumentExistsException)


def test_insert_multi_raises_without_return_exceptions(collection):
    collection.insert("taken", DOC)
    with pytest.raises(DocumentExistsException):
        collection.insert_multi({"taken": DOC}, return_exceptions=False)


def test_get_multi_missing_key(collection):
    collection.upsert("key", DOC)
    res = collection.get_multi(["key", "missing"])
    assert not res.all_ok
    assert res.results["key"].content_as[dict] == DOC
    assert set(res.results) == {"key"}
    assert isinstance(res.exceptions["missing"], DocumentNotFoundException)


@pytest.mark.parametrize(
    "kwargs",
    [{"return_exceptions": False}, {"opts": GetMultiOptions(return_exceptions=False)}],
)
def test_get_multi_missing_key_raises_without_return_exceptions(collection, kwargs):
    with pytest.raises(DocumentNotFoundException):
        collection.get_multi(["missing"], **kwargs)


def test_remove_multi(collection):
    collection.upsert("key", DOC)
    res = collection.remove_multi(["key", "missing"])
    assert not res.all_ok
    assert set(res.results) == {"key"}
    assert isinstance(res.exceptions["missing"], DocumentNotFoundException)


def test_wait_until_ready_returns_promptly(local_timezone, cluster):
    start = time.monotonic()
    cluster.wait_until_ready(timedelta(seconds=5))
    assert time.monotonic() - start < 1


@pytest.mark.parametrize("expiry", [60, timedelta(seconds=60)])
def test_expiry_is_epoch_seconds(local_timezone, collection, expiry):
    expected = time.time() + 60
    assert collection._get_expiry(expiry=expiry) == pytest.approx(expected, abs=2)


def test_document_expires(local_timezone, collection, monkeypatch):
    collection.upsert("key", DOC, expiry=timedelta(seconds=30))
    assert collection.get("key").content_as[dict] == DOC

    now = time.time()
    monkeypatch.setattr(fake_couchbase.store, "time", lambda: now + 31)
    with pytest.raises(DocumentNotFoundException):
        collection.get("key")


def test_touch_extends_expiry(local_timezone, collection, monkeypatch):
    collection.upsert("key", DOC, expiry=timedelta(seconds=30))
    collection.touch("key", expiry=timedelta(seconds=120))

    now = time.time()
    monkeypatch.setattr(fake_couchbase.store, "time", lambda: now + 60)
    assert collection.get("key").content_as[dict] == DOC


def _stored_expiry(collection, key):
    return fake_couchbase.collection._STORE._documents[collection._store_name][key]["expiry"]


@pytest.mark.parametrize(
    "call",
    [
        pytest.param(lambda c: c.upsert("key", DOC, expiry=30), id="kwarg"),
        pytest.param(
            lambda c: c.upsert("key", DOC, UpsertOptions(expiry=timedelta(seconds=30))),
            id="positional-options",
        ),
        # How python-couchbase-helper calls it.
        pytest.param(
            lambda c: c.upsert(
                key="key", value=DOC, opts=UpsertOptions(expiry=timedelta(seconds=30))
            ),
            id="opts-kwarg",
        ),
        pytest.param(
            lambda c: c.upsert("key", DOC, UpsertOptions(expiry=timedelta(hours=1)), expiry=30),
            id="kwarg-overrides-options",
        ),
    ],
)
def test_expiry_from_options(collection, call):
    call(collection)
    assert _stored_expiry(collection, "key") == pytest.approx(time.time() + 30, abs=2)


def test_zero_expiry_means_no_expiry(collection):
    collection.upsert("key", DOC, expiry=0)
    assert _stored_expiry(collection, "key") == 0
    assert collection.get("key").content_as[dict] == DOC


def test_multi_expiry_from_options(collection):
    options = UpsertMultiOptions(
        expiry=timedelta(seconds=30),
        per_key_options={"b": UpsertOptions(expiry=timedelta(seconds=90))},
    )
    collection.upsert_multi({"a": DOC, "b": DOC}, opts=options)
    assert _stored_expiry(collection, "a") == pytest.approx(time.time() + 30, abs=2)
    assert _stored_expiry(collection, "b") == pytest.approx(time.time() + 90, abs=2)


@pytest.mark.parametrize("lock_time", [15, timedelta(seconds=15)])
def test_get_and_lock(collection, lock_time):
    collection.upsert("key", DOC)
    assert collection.get_and_lock("key", lock_time).content_as[dict] == DOC

    # Reads still work on a locked document; writes and a second lock do not.
    assert collection.get("key").content_as[dict] == DOC
    assert collection.exists("key").exists is True
    for write in (
        lambda: collection.upsert("key", DOC),
        lambda: collection.replace("key", DOC),
        lambda: collection.remove("key"),
        lambda: collection.touch("key", expiry=30),
        lambda: collection.get_and_lock("key", lock_time),
    ):
        with pytest.raises(DocumentLockedException):
            write()


def test_unlock(collection):
    collection.upsert("key", DOC)
    collection.get_and_lock("key", 15)
    collection.unlock("key", 0)
    collection.replace("key", {"replaced": True})
    assert collection.get("key").content_as[dict] == {"replaced": True}


def test_lock_expires(collection, monkeypatch):
    collection.upsert("key", DOC)
    collection.get_and_lock("key", 15)

    now = time.time()
    monkeypatch.setattr(fake_couchbase.store, "time", lambda: now + 16)
    collection.replace("key", {"replaced": True})


@pytest.mark.parametrize("operation", ["get_and_lock", "unlock"])
def test_lock_missing_document(collection, operation):
    with pytest.raises(DocumentNotFoundException):
        getattr(collection, operation)("missing", 15)
