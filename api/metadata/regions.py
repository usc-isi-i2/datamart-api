
from flask import request
from flask_restful import Resource
from db.sql.utils import query_to_dicts

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
            return self.get_countries()

        # Admin1s of country
        if 'country' in args or 'country_id' in args:
            return self.get_admin1s(args.get('country'), args.get('country_id'))

        # Admin2s of admin1
        if 'admin1' in args or 'admin1_id' in args:
            return self.get_admin2s(args.get('admin1'), args.get('admin1_id'))

        # Admin3s of admin2
        if 'admin2' in args or 'admin2_id' in args:
            return self.get_admin3s(args.get('admin2'), args.get('admin2_id'))
        
        return {}

    def get_countries(self, country=None, country_id=None):
        query = f'''
        SELECT e_country.node1 AS country_id,
               s_country_label.text AS country,
               NULL as admin1_id,
               NULL as admin1,
               NULL as admin2_id,
               NULL as admin2,
               NULL as admin3_id,
               NULL as admin3
        FROM edges e_country
            JOIN edges e_country_label JOIN strings s_country_label ON (e_country_label.id=s_country_label.edge_id)
                ON (e_country.node1=e_country_label.node1 AND e_country_label.label='label')
        WHERE e_country.label='P31' AND e_country.node2='Q6256'
        ORDER BY country
        '''

        return query_to_dicts(query)

    def get_admin1s(self, country=None, country_id=None):
        if not country and not country_id:
            raise ValueError('Either country or country_id must be supplied')
        if country and country_id:
            raise ValueError('Only one of country, country_id may be specified')

        if country_id:
            where = f"e_country.node2='{country_id}'"
        else:
            where = f"LOWER(s_country_label.text)=LOWER('{country}')"

        query = f'''
        SELECT e_country.node2 AS country_id,
               s_country_label.text AS country,
               e_admin1.node1 as admin1_id,
               s_admin1_label.text as admin1,
               NULL as admin2_id,
               NULL as admin2,
               NULL as admin3_id,
               NULL as admin3
        FROM edges e_admin1
            JOIN edges e_admin1_label JOIN strings s_admin1_label ON (e_admin1_label.id=s_admin1_label.edge_id)
				ON (e_admin1.node1=e_admin1_label.node1 AND e_admin1_label.label='label')
            JOIN edges e_country ON (e_country.node1=e_admin1.node1 AND e_country.label='P17')
            JOIN edges e_country_label JOIN strings s_country_label ON (e_country_label.id=s_country_label.edge_id)
                ON (e_country.node2=e_country_label.node1 AND e_country_label.label='label')
        WHERE e_admin1.label='P31' AND e_admin1.node2='Q10864048' AND {where}
        ORDER BY admin1
        '''

        return query_to_dicts(query)

    def get_admin2s(self, admin1=None, admin1_id=None):
        if not admin1 and not admin1_id:
            raise ValueError('Either admin1 or admin1_id must be supplied')
        if admin1 and admin1_id:
            raise ValueError('Only one of admin1, admin1_id may be specified')

        if admin1_id:
            where = f"e_admin1.node2='{admin1_id}'"
        else:
            where = f"LOWER(s_admin1_label.text)=LOWER('{admin1}')"

        query = f'''
        SELECT e_country.node2 AS country_id,
               s_country_label.text AS country,
               e_admin1.node2 AS admin1_id,
               s_admin1_label.text AS admin1,
               e_admin2.node1 AS admin2_id,
               s_admin2_label.text AS admin2,
               NULL as admin3_id,
               NULL as admin3
        FROM edges e_admin2
            JOIN edges e_admin2_label JOIN strings s_admin2_label ON (e_admin2_label.id=s_admin2_label.edge_id)
				ON (e_admin2.node1=e_admin2_label.node1 AND e_admin2_label.label='label')
            JOIN edges e_admin1 ON (e_admin1.node1=e_admin2.node1 AND e_admin1.label='P2006190001')
            JOIN edges e_admin1_label JOIN strings s_admin1_label ON (e_admin1_label.id=s_admin1_label.edge_id)
				ON (e_admin1.node2=e_admin1_label.node1 AND e_admin1_label.label='label')
            JOIN edges e_country ON (e_country.node1=e_admin1.node2 AND e_country.label='P17')
            JOIN edges e_country_label JOIN strings s_country_label ON (e_country_label.id=s_country_label.edge_id)
                ON (e_country.node2=e_country_label.node1 AND e_country_label.label='label')
        WHERE e_admin2.label='P31' AND e_admin2.node2='Q13220204' AND {where}
        ORDER BY admin2
        '''

        return query_to_dicts(query)

    def get_admin3s(self, admin2=None, admin2_id=None):
        if not admin2 and not admin2_id:
            raise ValueError('Either admin2 or admin2_id must be supplied')
        if admin2 and admin2_id:
            raise ValueError('Only one of admin2, admin2_id may be specified')

        if admin2_id:
            where = f"e_admin2.node2='{admin2_id}'"
        else:
            where = f"LOWER(s_admin2_label.text)=LOWER('{admin2}')"

        query = f'''
        SELECT e_country.node2 AS country_id,
               s_country_label.text AS country,
               e_admin1.node2 AS admin1_id,
               s_admin1_label.text AS admin1,
               e_admin2.node2 AS admin2_id,
               s_admin2_label.text AS admin2,
               e_admin2.node1 AS admin3_id,
               s_admin3_label.text AS admin3
        FROM
            edges e_admin3
            JOIN edges e_admin3_label JOIN strings s_admin3_label ON (e_admin3_label.id=s_admin3_label.edge_id)
				ON (e_admin3.node1=e_admin3_label.node1 AND e_admin3_label.label='label')
            JOIN edges e_admin2 ON (e_admin2.node1=e_admin3.node1 AND e_admin2.label='P2006190002')
            JOIN edges e_admin2_label JOIN strings s_admin2_label ON (e_admin2_label.id=s_admin2_label.edge_id)
				ON (e_admin2.node2=e_admin2_label.node1 AND e_admin2_label.label='label')
            JOIN edges e_admin1 ON (e_admin1.node1=e_admin2.node1 AND e_admin1.label='P2006190001')
            JOIN edges e_admin1_label JOIN strings s_admin1_label ON (e_admin1_label.id=s_admin1_label.edge_id)
				ON (e_admin1.node2=e_admin1_label.node1 AND e_admin1_label.label='label')
            JOIN edges e_country ON (e_country.node1=e_admin1.node2 AND e_country.label='P17')
            JOIN edges e_country_label JOIN strings s_country_label ON (e_country_label.id=s_country_label.edge_id)
                ON (e_country.node2=e_country_label.node1 AND e_country_label.label='label')
        WHERE e_admin3.label='P31' AND e_admin3.node2='Q13221722' AND {where}
        ORDER BY admin3
        '''
        print(query)

        return query_to_dicts(query)

