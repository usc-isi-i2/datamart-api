from flask_restful import Resource
from .get import VariableGetter
from .put import PutCanonicalData


class VariableResource(Resource):
    def get(self, dataset=None, variable=None):
        imp = VariableGetter()
        return imp.get(dataset, variable)

    def put(self, dataset, variable):
        put_data = PutCanonicalData()
        return put_data.canonical_data(dataset, variable)
