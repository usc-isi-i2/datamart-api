# Build a Docker image for the backend.
# This image serves the backend from port 80, using gunicorn.
# You need to pass the proper database URI in the SQLALCHEMY_DATABASE_URI environment variable,
# or a local, very transient sqlite instance is going to be used.

FROM python:3.7
RUN pip install --upgrade pip
RUN pip install gunicorn

COPY requirements.txt /src/requirements.txt

RUN pip install -r src/requirements.txt && pip uninstall -y torch

# RUN git clone https://github.com/usc-isi-i2/t2wml-annotation && cd t2wml-annotation && git checkout 1c750e99ad34467d85ff478603a338015bbfe67f
RUN git clone https://github.com/usc-isi-i2/t2wml-annotation && cd t2wml-annotation && git checkout 5371244918018a9c566ac1265cb74c3edee07cfd

# Uninstall unnecessary packages
RUN pip install -e t2wml-annotation && pip uninstall -y torch

# RUN git clone https://github.com/usc-isi-i2/t2wml-api && cd t2wml-api && git checkout causeex
RUN git clone https://github.com/usc-isi-i2/t2wml-api && cd t2wml-api && git checkout bceea3fc18a29f33f2a03e5011e0d22c93a03b9f
RUN pip install -e t2wml-api

# Something installs an old kgtk, we need the new one
# RUN pip install --upgrade kgtk

## Install psql, assuming OS version "buster"
# sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
# wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
# sudo apt-get update
# sudo apt-get -y install postgresql
RUN echo "deb http://apt.postgresql.org/pub/repos/apt buster-pgdg main" > /etc/apt/sources.list.d/pgdg.list
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN apt-get update
RUN apt-get -y install postgresql

COPY . /src
RUN mkdir -p /src/instance
COPY docker/docker_config.py /src/instance/config.py
COPY dev-env/data/postgres/causx.sql.gz /src/dev-env/data/postgres/causx.sql.gz
WORKDIR /src

# CMD ["gunicorn", "-b", "0.0.0.0:80", "--timeout", "3600", "wsgi:app"]
CMD ["bash", "docker/initialize-datamart.sh"]
