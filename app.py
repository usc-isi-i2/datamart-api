import os.path

from flask import Flask
from flask_restful import Api
from flask_cors import CORS

import api.hello
from api.variable import VariableResource, VariableResourceAll
from api.metadata import DatasetMetadataResource, VariableMetadataResource, FuzzySearchResource, RegionSearchResource

app = Flask(__name__)
CORS(app)

app.config.from_pyfile('config.py')
instance_config_path = os.path.join('instance', 'config.py')
if os.path.isfile(instance_config_path):
    app.config.from_pyfile(instance_config_path)

app.register_blueprint(api.hello.bp)
api = Api(app)
api.add_resource(VariableResource, '/datasets/<string:dataset>/variables/<string:variable>')
api.add_resource(VariableResourceAll, '/datasets/<string:dataset>/variables')
api.add_resource(DatasetMetadataResource, '/metadata/datasets', '/metadata/datasets/<string:dataset>')
api.add_resource(VariableMetadataResource, '/metadata/datasets/<string:dataset>/variables',
                 '/metadata/datasets/<string:dataset>/variables/<string:variable>')
api.add_resource(FuzzySearchResource, '/metadata/variables')
api.add_resource(RegionSearchResource, '/metadata/regions')

if __name__ == '__main__':
    app.run()
