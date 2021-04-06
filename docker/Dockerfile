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

# Something installs an old kgtk, we need the new one
RUN pip install --upgrade kgtk

COPY . /src
RUN mkdir -p /src/instance
COPY docker/docker_config.py /src/instance/config.py
WORKDIR /src

CMD ["gunicorn", "-b", "0.0.0.0:80", "--timeout", "3600", "wsgi:app"] 
