from flask_restful import Resource
from .get import VariableGetter
from .put import CanonicalData
from .delete import VariableDeleter


class VariableResource(Resource):
    def get(self, dataset=None, variable=None):
        imp = VariableGetter()
        return imp.get(dataset, variable)

    def put(self, dataset, variable):
        put_data = CanonicalData()
        return put_data.canonical_data(dataset, variable)

    def post(self, dataset, variable):
        put_data = CanonicalData()
        return put_data.canonical_data(dataset, variable, is_request_put=False)

    def delete(self, dataset, variable):
        imp = VariableDeleter()
        return imp.delete(dataset, variable)


class VariableResourceAll(Resource):
    def get(self, dataset=None):
        from .get_all import VariableGetterAll  # Resolve circular import

        g = VariableGetterAll()
        return g.get(dataset)
