# This script refreshes the views required by the datamart-api.
import argparse

from config import POSTGRES
from db.sql.search_views import refresh_all_views
from db.sql.utils import postgres_connection

def run():
    print('Refreshing all materialized views')
    config = dict(POSTGRES=POSTGRES)
    refresh_all_views(config, True)
    print('Done')

if __name__ == '__main__':
    run()
