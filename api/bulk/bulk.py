from flask import send_from_directory
from pathlib import Path
import os


class BulkDataResource(object):
    def get(self):
        c_path = Path(__file__)
        dump_dir_path = c_path.parent.parent.parent.absolute()
        dump_path = f'{dump_dir_path}/dumps/datamart_datasets_dump.tar.gz'
        if os.path.exists(dump_path):
            return send_from_directory(f'{dump_dir_path}/dumps', 'datamart_datasets_dump.tar.gz', as_attachment=True,
                                       attachment_filename='datamart_datasets_dump.tar.gz')
        return {'Error': 'No Datamart dumps are available right now'}
