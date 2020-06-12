# A configuration file for the Docker image
POSTGRES = dict(
    database = 'wikidata',
    host = 'db',
    port = 5432,
    user = 'postgres',
    password = 'postgres',
)

METADATA_DIR = '/src/metadata'