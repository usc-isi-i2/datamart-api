# Build a Docker image for the backend.
# This image serves the backend from port 80, using gunicorn.
# You need to pass the proper database URI in the SQLALCHEMY_DATABASE_URI environment variable,
# or a local, very transient sqlite instance is going to be used.

FROM python:3.7
RUN pip install --upgrade pip
RUN pip install gunicorn

COPY requirements.txt /src/requirements.txt

RUN pip install -r src/requirements.txt && pip uninstall -y torch

# Commit hash is head of causeex branch
RUN git clone https://github.com/usc-isi-i2/t2wml-annotation && cd t2wml-annotation && git checkout 9904fa5fb54965ddbaecc6b04c8e283566d4ad0f


# Uninstall unnecessary packages
RUN pip install -e t2wml-annotation && pip uninstall -y torch

# Commit hash is head of development branch
RUN git clone https://github.com/usc-isi-i2/t2wml-api && cd t2wml-api && git checkout 6f4f1ebe6c46d493bf25d29e17dfdb54cc179432
RUN pip install -e t2wml-api

## Install psql, assuming OS version "buster"
# sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
# wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
# sudo apt-get update
# sudo apt-get -y install postgresql
RUN echo "deb http://apt.postgresql.org/pub/repos/apt buster-pgdg main" > /etc/apt/sources.list.d/pgdg.list
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN apt-get update
RUN apt-get -y install postgresql

RUN pip list

COPY . /src
RUN mkdir -p /src/instance
COPY docker/docker_config.py /src/instance/config.py
COPY dev-env/data/postgres/causx.sql.gz /src/dev-env/data/postgres/causx.sql.gz
WORKDIR /src

# CMD ["gunicorn", "-b", "0.0.0.0:80", "--timeout", "3600", "wsgi:app"]
CMD ["bash", "docker/initialize-datamart.sh"]
