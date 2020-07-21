from flask_restful import Resource
from .post import AnnotatedData


class AnnotatedResource(Resource):
    def post(self, dataset):
        post_data = AnnotatedData()
        return post_data.process(dataset)
