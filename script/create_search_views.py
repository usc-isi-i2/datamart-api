# This script creates the views required by the datamart-api.
# The may be moved into the postgres-wikidata project if they prove to be useful enough.
import argparse
import os
import sys

# Allow running from the command line - python script/import... doesn't add the root project directory
# to the PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config import POSTGRES
from db.sql.search_views import (create_view, does_view_exists, drop_view,
                                 get_view_name, ADMIN_TYPES)
from db.sql.utils import postgres_connection


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--recreate', action='store_true', default=False, help='Recreates existing views')

    return parser.parse_args()


def run():
    print('Creating the fuzzy search views')

    args = parse_args()

    config = dict(POSTGRES=POSTGRES)
    with postgres_connection(config) as conn:
        for admin, admin_pnode in ADMIN_TYPES.items():
            if does_view_exists(conn, admin):
                if args.recreate:
                    drop_view(conn, admin, debug=True)
                else:
                    print(f'View for {admin} already exists, skipping')
                    continue
            create_view(conn, admin, admin_pnode, debug=True)

    print('Done')

if __name__ == '__main__':
    run()
