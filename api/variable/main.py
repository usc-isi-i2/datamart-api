
from flask_restful import Resource
from .get import VariableGetter


class VariableResource(Resource):
    def get(self, dataset=None, variable=None):
        imp = VariableGetter()
        return imp.get(dataset, variable)

