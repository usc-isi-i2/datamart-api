# Build a Docker image for the backend.
# This image serves the backend from port 80, using gunicorn.
# You need to pass the proper database URI in the SQLALCHEMY_DATABASE_URI environment variable,
# or a local, very transient sqlite instance is going to be used.

FROM python:3.7

RUN pip install gunicorn

COPY requirements.txt /src/requirements.txt
RUN pip install -r src/requirements.txt
RUN cd /src && git clone https://github.com/usc-isi-i2/table-linker.git && cd table-linker && git checkout 21446966df409110af50098bacc91100b2521cd4 && pip install -e .
 
COPY . /src
RUN mkdir -p /src/instance
COPY docker/docker_config.py /src/instance/config.py
WORKDIR /src

CMD sh /src/docker/datamart-start.sh
