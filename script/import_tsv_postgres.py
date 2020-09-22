from argparse import ArgumentParser
import sys
import os

# Allow running from the command line - python script/import... doesn't add the root project directory
# to the PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from db.sql.kgtk import import_kgtk_tsv

config = dict(
    database = 'wikidata',
    host = 'localhost',
    port = 5433,
    user = 'postgres',
    password = 'postgres',
)
if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("input_file_path", help="input file", type=str)

    parsed = parser.parse_args()
    import_kgtk_tsv(parsed.input_file_path, config=config)
