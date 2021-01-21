# Build a Docker image for the backend.
# This image serves the backend from port 80, using gunicorn.
# You need to pass the proper database URI in the SQLALCHEMY_DATABASE_URI environment variable,
# or a local, very transient sqlite instance is going to be used.

FROM python:3.7
RUN pip install --upgrade pip
RUN pip install gunicorn

COPY requirements.txt /src/requirements.txt
RUN pip install -r src/requirements.txt
#RUN git clone https://github.com/usc-isi-i2/table-linker && cd table-linker && git checkout jun_devel
#RUN pip install -e table-linker

RUN git clone https://github.com/usc-isi-i2/t2wml-annotation && cd t2wml-annotation
RUN pip install -e t2wml-annotation

RUN git clone https://github.com/usc-isi-i2/t2wml-api && cd t2wml-api && git checkout development
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

COPY . /src
RUN mkdir -p /src/instance
COPY docker/docker_config.py /src/instance/config.py
WORKDIR /src

# CMD ["gunicorn", "-b", "0.0.0.0:80", "--timeout", "3600", "wsgi:app"]
CMD ["bash", "docker/initialize-datamart.sh"]
