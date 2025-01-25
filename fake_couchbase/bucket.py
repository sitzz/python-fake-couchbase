from couchbase.logic.bucket import BucketLogic

from fake_couchbase.collection import Collection
from fake_couchbase.scope import Scope


class Bucket(BucketLogic):
    def __init__(self, cluster, bucket_name):
        super().__init__(cluster, bucket_name)
        self._connected = False

    def close(self):
        if self.connected:
            super()._open_or_close_bucket(open_bucket=False)
            self._destroy_connection()

    def default_scope(self):
        return self.scope(Scope.default_name())

    def scope(self, name):
        return Scope(self, name)

    def collection(self, collection_name):
        scope = self.default_scope()
        return scope.collection(collection_name)

    def default_collection(self):
        scope = self.default_scope()
        return scope.collection(Collection.default_name())

    def ping(self):
        pass

    def view_query(self, design_doc, view_name, *view_options, **kwargs):
        pass

    def collections(self):
        # return CollectionManager(self.connection, self.name)
        pass

    def view_indexes(self):
        # return ViewIndexManager(self.connection, self.name)
        pass
