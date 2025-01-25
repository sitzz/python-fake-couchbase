from couchbase.logic.scope import ScopeLogic

from fake_couchbase.collection import Collection


class Scope(ScopeLogic):
    def __init__(self, bucket, scope_name):
        super().__init__(bucket, scope_name)

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
