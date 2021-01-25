#!/bin/bash

# export POSTGRES_HOST=localhost
# export POSTGRES_PORT=6000
# export POSTGRES_PASSWORD=empty
# export POSTGRES_USER=auser
# export POSTGRES_DB=wikidata2

echo POSTGRES_HOST = $POSTGRES_HOST
echo POSTGRES_PORT = $POSTGRES_PORT
echo POSTGRES_PASSWORD = $POSTGRES_PASSWORD
echo POSTGRES_USER = $POSTGRES_USER
echo POSTGRES_DB = $POSTGRES_DB

export PGPASSWORD=$POSTGRES_PASSWORD

until psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -c '\q' $POSTGRES_DB; do
    >&2 echo "Postgres is unavailable - sleeping"
    sleep 1
done

# If edges table does not exist, then create the datamart database
>&2 echo psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -tAc "select to_regclass('public.edges');" $POSTGRES_DB
result=$(psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -tAc "select to_regclass('public.edges');" $POSTGRES_DB)
>&2 echo "psql result: $result"
if [ "$result" != "edges" ]; then
    >&2 echo "Creating database $POSTGRES_DB"
    >&2 echo $(ls docker/dev-env/data/postgres/causx.sql)
    >&2 echo "zcat docker/dev-env/data/postgres/causx.sql.gz | psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER $POSTGRES_DB"
    # zcat docker/dev-env/data/postgres/causx.sql.gz | psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER $POSTGRES_DB
    zcat dev-env/data/postgres/causx.sql.gz | psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER $POSTGRES_DB
fi

gunicorn -b 0.0.0.0:80 --timeout 3600 wsgi:app
