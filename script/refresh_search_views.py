# This script refreshes the views required by the datamart-api.
import argparse
import os
import sys

# Allow running from the command line - python script/import... doesn't add the root project directory
# to the PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config import DB
from db.sql.search_views import refresh_all_views
from db.sql.utils import db_connection

def run():
    print('Refreshing all materialized views')
    config = dict(DB=DB)
    refresh_all_views(config, True)
    print('Done')

if __name__ == '__main__':
    run()
