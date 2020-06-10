import os.path

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

STORAGE_BACKEND = 'postgres'
POSTGRES = dict(
    database = 'wikidata',
    host = 'localhost',
    port = 5433,
    user = 'postgres',
    password = 'postgres',
)

METADATA_DIR = os.path.join(BASE_DIR, 'metadata')

