from argparse import ArgumentParser
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
    import_kgtk_tsv(parsed.input_file_path, config=conf-ig)
