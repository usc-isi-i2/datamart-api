import os
from requests import put


def upload_data_put(file_path, url):
    file_name = os.path.basename(file_path)
    files = {
        'file': (file_name, open(file_path, mode='rb'), 'application/octet-stream')
    }
    return put(url, files=files)
