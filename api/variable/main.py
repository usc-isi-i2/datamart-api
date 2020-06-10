
from flask_restful import Resource
from .get import VariableGetter
from .put import canonical_data


class VariableResource(Resource):
    def get(self, dataset=None, variable=None):
        imp = VariableGetter()
        return imp.get(dataset, variable)

    def put(self, dataset, variable):
        return canonical_data(dataset, variable)