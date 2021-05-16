import datetime
import json
import os.path

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

STORAGE_BACKEND = 'postgres'
POSTGRES = dict(
    database = 'wikidata',
    host = 'localhost',
    port = 5432,
    user = 'postgres',
    password = 'postgres',
)

METADATA_DIR = os.path.join(BASE_DIR, 'metadata')

RESTFUL_JSON = dict(
    cls = CustomEncoder,
)
