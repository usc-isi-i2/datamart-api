
from flask import request
from flask_restful import Resource
from db.sql.utils import query_to_dicts
from db.sql import dal

class InvalidArgumentsError(Exception):
    def __init__(self, err, *args, **kwargs):
        super().__init__(err, *args, **kwargs)
        self.error = err


class RegionSearchResource(Resource):
    def parse_args(self):
        # Parse arguments, allow just one argument
        allowed = ['country', 'country_id', 'admin1', 'admin1_id', 'admin2', 'admin2_id', 'admin3', 'admin3_id']  # list and not set, so the error has them in order
        allowed_args = { key: value for (key, value) in request.args.items() if key in allowed }

        if len(allowed_args) > 1:
            raise InvalidArgumentsError('Only one of the following arguments can be specified: ' +  ', '.join(allowed))

        return allowed_args

    def get(self):
        try:
            args = self.parse_args()
        except InvalidArgumentsError as ex:
            return { 'Error': ex.error }, 400

        # All countries
        if not args:
            return dal.query_countries()

        # Admin1s of country
        if 'country' in args or 'country_id' in args:
            return dal.query_admin1s(args.get('country'), args.get('country_id'))

        # Admin2s of admin1
        if 'admin1' in args or 'admin1_id' in args:
            return dal.query_admin2s(args.get('admin1'), args.get('admin1_id'))

        # Admin3s of admin2
        if 'admin2' in args or 'admin2_id' in args:
            return dal.query_admin3s(args.get('admin2'), args.get('admin2_id'))
        
        return {}