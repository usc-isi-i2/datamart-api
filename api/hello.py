from flask import Blueprint, current_app

bp = Blueprint('hello', __name__)

@bp.route('/')
def hello() -> str:
    # return f"Backend is {current_app.config['STORAGE_BACKEND']}"
    return f"<html><a href='https://datamart-upload.readthedocs.io/en/latest/'>Datamart Upload API</a></html>"