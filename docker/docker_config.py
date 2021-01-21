# A configuration file for the Docker image

import os

POSTGRES = dict(
    database = os.environ.get('POSTGRES_DB', 'wikidata'),
    host = os.environ.get('POSTGRES_HOST', 'db'),
    port = int(os.environ.get('POSTGRES_PORT', 5432)),
    user = os.environ.get('POSTGRES_USER', 'postgres'),
    password = os.environ.get('POSTGRES_PASSWORD', 'postgres')
)

METADATA_DIR = '/src/metadata'
