#!/bin/sh
cd /src
mv docker/upload_es_index.py ./
python upload_es_index.py
echo "Staring main Datamart service..."
gunicorn -b 0.0.0.0:80 --timeout 3600 wsgi:app