# This file runs datamart.
import os
import argparse

# datamart --db-host localhost --db-port 5433 --db-name wikidata --db-user postgres --db-password postgres

# To test, use Postman and issue a GET to http://localhost:12543/metadata/datasets

# To start the database, from Windows (not WSL) go to datamart-api/dev-env and run docker-compose up

# Clone this, to be a sibling of datamart-api https://github.com/usc-isi-i2/table-linker
# Go to the jun_devel branch
# pip install -e ../table-linker

parser = argparse.ArgumentParser()
parser.add_argument("--db-host", default="localhost", help="DB host")
parser.add_argument("--db-port", default="5433", help="DB port")
parser.add_argument("--db-name", default="wikidata", help="DB name")
parser.add_argument("--db-user", default="postgres", help="DB user")
parser.add_argument("--db-password", default="postgres", help="DB password")
args = parser.parse_args()

os.environ["DB_HOST"] = args.db_host
os.environ["DB_PORT"] = args.db_port
os.environ["DB_NAME"] = args.db_name
os.environ["DB_USER"] = args.db_user
os.environ["DB_PASSWORD"] = args.db_password

from app import app

if __name__ == '__main__':
    # print(app.config.get("POSTGRES"))
    app.run(port=12543)