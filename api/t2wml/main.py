from db.sql.kgtk import import_kgtk_tsv
from flask_restful import Resource
import tempfile
import os
from flask import request

# from .put import IngestT2WMLOutput


# class T2WMLResource(Resource):
#     def post(self, dataset):
#         post_data = IngestT2WMLOutput()
#         return post_data.process(dataset, is_request_put=False)
#
#     def put(self, dataset):
#         put_data = IngestT2WMLOutput()
#         return put_data.process(dataset)

class T2WMLResource(Resource):
    def post(self, dataset):
        if 'edges.tsv' not in request.files:
            return { 'Error': 'Please pass a file called "edges.tsv"' }, 400

        edges = request.files['edges.tsv']

        tmp_filename = tempfile.mktemp('.tsv')
        try:
            edges.save(tmp_filename)
            import_kgtk_tsv(tmp_filename, replace=True)
        finally:
            try:
                os.remove(tmp_filename)
            except:
                pass  # Don't fail if temp file can't be deleted

        return { }, 204
