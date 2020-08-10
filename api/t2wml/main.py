from flask_restful import Resource
from .put import IngestT2WMLOutput


class T2WMLResource(Resource):
    def post(self, dataset):
        post_data = IngestT2WMLOutput()
        return post_data.process(dataset, is_request_put=False)

    def put(self, dataset):
        put_data = IngestT2WMLOutput()
        return put_data.process(dataset)
