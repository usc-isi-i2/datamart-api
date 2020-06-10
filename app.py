from flask import Flask
import api.hello

app = Flask(__name__)
app.register_blueprint(api.hello.bp)

if __name__=='__main__':
    app.run()
