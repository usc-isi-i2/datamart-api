# This file runs datamart.

from app import app

# TODO: Get the postgres information from the commandline (using argparse)
# and update the config
#
# datamart --db-host localhost --db-port 5433 --db-name wikidata --db-user postgres --db-password postgres

# To test, use Postman and issue a GET to http://localhost:12543/metadata/datasets
# To start the database, from Windows (not WSL) go to datamart-api/dev-env and run docker-compose up

# Clone this, to be a sibling of datamart-api https://github.com/usc-isi-i2/table-linker
# Go to the jun_devel branch
# pip install -e ../table-linker

if __name__ == '__main__':
    app.run(port=12543)