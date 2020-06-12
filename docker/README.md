# A Dockerized Datamart API
This folder contains everything you need to run a Dockerized version of the Datamart API.

## Building the container
To build the container, simply switch to this directory and run

    docker-compose build

This will build the backend container. It may take a while the first time you do it, as there are *a lot* of Python packages that need to be installed. Every time you change the source you should build the container again. Subsequent building runs will be faster - unless you change `requirements.txt`

## Running
To run, simply run

    docker-compose up

Note that if you already have the `dev-env` docker-compose up, you will not be able to run this docker-compose. Shut down the dev-env's docker-compose. If docker-compose complains that the postgres volume is still used, type `docker container prune` to remove left-overs from the dev-env docker-compose.

## Using the dockerized API
The dockerized Datamart API is available at localhost:14080
