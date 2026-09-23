from fake_couchbase._compat import ScopeBase
from fake_couchbase.collection import Collection


class Scope(ScopeBase):
    def __init__(self, bucket, scope_name):
        super().__init__(bucket, scope_name)

    @staticmethod
    def default_name():
        return "_default"

    def collection(self, name):
        return Collection(self, name)

    def query(self, statement, *options, **kwargs):
        pass

    def analytics_query(self, statement, *options, **kwargs):
        pass

    def search_query(self, index, query, *options, **kwargs):
        pass

    def search(self, index, request, *options, **kwargs):
        pass

    def search_indexes(self):
        pass

    def eventing_functions(self):
        pass
