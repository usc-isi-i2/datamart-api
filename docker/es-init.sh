#!/bin/sh
cd /src
mv docker/upload_es_index.py ./
python upload_es_index.py
echo "Elastic Search initialization finished, now exiting..."
exit 0