import datetime
import json
import os

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

STORAGE_BACKEND = 'postgres'

POSTGRES = dict(
   host = os.environ.get("DB_HOST", default="localhost"),
   port = os.environ.get("DB_PORT", default="5432"),
   database = os.environ.get("DB_NAME", default="wikidata"),
   user = os.environ.get("DB_USER", default="postgres"),
   password = os.environ.get("DB_PASSWORD", default="postgres"),
)

METADATA_DIR = os.path.join(BASE_DIR, 'metadata')

RESTFUL_JSON = dict(
    cls = CustomEncoder,
)
