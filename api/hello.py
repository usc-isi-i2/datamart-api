from flask import Blueprint

bp = Blueprint('hello', __name__)

@bp.route('/')
def hello() -> str:
    return 'Hello blueprint'