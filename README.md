# Datamart-API

## Setting up for development
First clone this repo. *IMPORTANT* this repo uses git-lfs, make sure you have it installed on your computer.

Now set up a virtual environment and activate it (we usually set it up in ./env). Install the requirements.

You need to configure Visual Studio Code to use the environment. Copy `.vscode/settings.linux.json` (for Linux and Macs, or `settings.windows.json` if you're using Windows) to `.vscode/settings.json` and set the `pythonPath` property to point to your virtual environment.

## Folder structure
The main directory contains the basic Flask files - a thin app.py, wsgi.py and config.py. All other functionality is placed in packages. We have the following subdirectories

* api - a package containing Flask Blueprints and provides the API endpoints. There are no endpoints in app.py, just blueprint registration.
* metadata - containing metadata configuration files
* db/sql - a package containing all Postgres related files (SQLAlchemy models, helper functions etc...)
* db/sparql - a package containing all SPARQL related files (empty at this stage)
* dev-env - a folder with Docker files to set up a development environment (Postgres and backup files)

## Running the development envirnoment dependencies (Postgres)
To run Postgres for development, simple switch to the `dev-env` directory and run

    docker-compose up

This will set up the database and make it available on port 5433. You can see the credentials in `docker-compose.yml`. These credentials are also the default configuration, so unless you change them, the system should just work.

*IMPORTANT* do not use the default configuration in production - choose different login credentials

### Updating the database content
This repo contains a backup of the UAZ data, to ease development. If the data has been changed and you want to load the new data, you need to delete the Postgres volume and restart it:

    docker-compose down --volumes
    docker-compose up

