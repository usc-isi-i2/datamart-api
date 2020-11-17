from flask_restful import Resource
from .bulk import BulkDataResource


class BulkResource(Resource):
    def get(self, dataset=None, variable=None):
        bdr = BulkDataResource()
        return bdr.get()
