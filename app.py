from flask import Flask
import api.hello
import os.path

app = Flask(__name__)
app.config.from_pyfile('config.py')

instance_config_path = os.path.join('instance', 'config.py')
if os.path.isfile(instance_config_path):
    app.config.from_pyfile(instance_config_path)

app.register_blueprint(api.hello.bp)

if __name__=='__main__':
    app.run()
