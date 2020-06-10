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

