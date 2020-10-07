# Managing Postgres for Datamart

Datamart uses Postgres for its data storage. This document explains how to set Postgres up, and how to perform some operations on the data.

## Database related configuration
The `config.py` file in this directory contains the database parameters - host, port, database name, etc... It is set up to work with the database configured in `dev-env/docker-compose.yml` and `docker/docker-compose.yml` . If you want to override these settings, create an `instance` directory, and place a copy of `config.py` with updated parameters.

## Database Models and Schema
We have SQLAlchemy models for the database tables. These are located in `db/sql/models.py` . We have used `alembic` to create the database schema. You can find the resulting statements in `db/sql/create-schema.sql` .

## Prepared database backups
We have two database backups ready for restoration and deployment. These backups contain all the tables, the fuzzy search views and data.

1. Base
The Base backup contains the tables, views, basic properties, all the regions and the FSI dataset. This backup should be used for a speedy set up of Datamart, if you plan to add your own datasets. The kgtk files for this backup are in `db/sql/data/base`. The backup file itself is in `db/sql/data/base.sql.gz` .

2. Full
The Full backup contains everything in base, as well as the datasets in the following table: https://docs.google.com/spreadsheets/d/17V3QMUGdm3TW4vQKcM-Zhm9tNUJVLSX02D1SmF_pF3E/edit#gid=0 .

The docker-compose files in `dev-env/` and `docker/` both load the full backup automatically.

## Loading data into the database

We have utilities for importing exploded kgtk edge files into the database - a simple method for small files and a bulk-copy method that is a bit more involved, but considerably faster.

### Plain import
Simply run

```sh
cd script
python import_tsv_postgres.py <kgtk-edge-file-name>
```

### Bulk copy
Postgres has a quick bulk-copy feature that works considerably faster than ordinary INSERT statements. Using it requires two steps - preparing the files for bulk import and actually running the bulk import.

To prepare for import, place all the kgtk files you want to import in one directory and run

```sh
cd script
python script/prepare_for_bulk.py <kgtk-edge-files> --output-dir <tsv-output-dir>
```

The script takes the KGTK edge files, compares their content to the existing database (to avoid inserting duplicates) and creates TSV files that can be imported to Postgres using its bulk copy facility. Note that the output directory should be accessible to Postgres. If Postgres is running inside docker, this directory needs to be mounted into the docker container.

After preparation you can perform the bulk copy. Run

```sh
python script/bulk_copy.py <tsv-output-dir>`
```

`tsv-output-dir` should be the path as Postgres sees it.

#### Bulk copy example
We can bulk import the base data into an empty database, running in one of the docker-compose files in this repo (in `dev-env` or `docker`).

The exploded kgtk files are in `db/sql/data/base`. The directory `dev-env/data/postgres` is mapped into the Postgres docker image under `/backup`. To import the base files do the following (always from the root directory of this repo):

1. Create the directory Postgres will use for bulk copy:

```sh
mkdir dev-env/data/postgres/for-import
```

(this directory will be called `/backup/for-import` inside Postgres)

2. Prepare the kgtk files for bulk copy

```sh
python script/prepare_for_bulk.py db/sql/data/base/*.tsv --output-dir  dev-env/data/postgres/for-import
```

3. Bulk copy into Postgres
python script/bulk_copy.py /backup/for-import

## Fuzzy Search Views
For implement part of the fuzzy search, we need materialized views in Postgres. These are created by

```sh
python script/create_search_views.py
```

The views are materialized, and need to be refreshed whenever data is added to Postgres. You refresh the views by running

```sh
python script/refresh_search_views.py
```

## Handling Postgres inside Docker
To handle Postgres inside Docker, you should open a shell inside the container. Do so by running

```sh
docker exec -it datamart-postgres /bin/bash
```

To run a backup, from inside the docker container run

```
cd /backup
pg_dump --username postgres --postgres --dbname wikidata | gzip > backup.sql.gz
```

You will be prompted for the postgres user password, which is `postgres` in our docker images.
