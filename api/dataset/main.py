from flask_restful import Resource
from .post import DatasetData


class DatasetResource(Resource):
    def post(self, dataset):
        raise NotImplementedError("kgtk support has been thorougly disabled")
        post_data = DatasetData()
        return post_data.process(dataset)

    def put(self, dataset):
        return {'Error': 'The PUT method is not allowed.'}
        
    def get(self, dataset):
        return {'Error': 'The GET method is currently not implemented.'
                         f" Use the endpoint 'datasets/{dataset}/variables' instead"}
