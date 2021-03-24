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
    parser.add_argument("--delete", help="Delete edges from the database", default=False, action="store_true")
    parser.add_argument("--replace", help="Replace existing edges in the database", default=False, action="store_true")

    parsed = parser.parse_args()
    if parsed.delete and parsed.replace:
        print("Can't specify both --delete and --replace", out=sys.stderr)
    else:
        import_kgtk_tsv(parsed.input_file_path, config=config, delete=parsed.delete, replace= parsed.replace)
