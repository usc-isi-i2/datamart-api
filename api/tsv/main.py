from flask_restful import Resource
from .post import TsvData


class TsvResource(Resource):
    def post(self, dataset):
        post_data = TsvData()
        return post_data.process(dataset)

    def put(self, dataset):
        put_data = TsvData()
        return put_data.process(dataset, is_request_put=True)
