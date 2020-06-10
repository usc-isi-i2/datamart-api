from flask import Blueprint, current_app

bp = Blueprint('hello', __name__)

@bp.route('/')
def hello() -> str:
    return f"Number is {current_app.config['NUMBER']}, dict number is {current_app.config['DICT']['number']}: {current_app.config['DICT']['message']}"