from typing import Dict, List, Optional
from db.sql.dal.general import sanitize
from db.sql.utils import query_to_dicts


class Region:
    admin: str
    admin_id: str
    region_type: str
    country: str
    country_id: str
    admin1: Optional[str]
    admin1_id: Optional[str]
    admin2: Optional[str]
    admin2_id: Optional[str]
    admin3: Optional[str]
    admin3_id: Optional[str]
    region_coordinate: Optional[str]

    COUNTRY = 'Q6256'
    ADMIN1 = 'Q10864048'
    ADMIN2 = 'Q13220204'
    ADMIN3 = 'Q13221722'

    def __init__(self, **kwargs):
        self.admin = kwargs['admin']
        self.admin_id = kwargs['admin_id']
        self.region_type = kwargs['region_type']
        self.country = kwargs['country']
        self.country_id = kwargs['country_id']
        self.admin1 = kwargs.get('admin1')
        self.admin1_id = kwargs.get('admin1_id')
        self.admin2 = kwargs.get('admin2')
        self.admin2_id = kwargs.get('admin2_id')
        self.admin3 = kwargs.get('admin3')
        self.admin3_id = kwargs.get('admin3_id')
        self.region_coordinate = kwargs.get('region_coordinate')

        # country, admin1 and admin2 queries return both admin and country,admin1,admin2 fields.
        # admin3 queries do not, so we need to feel these fields ourselves
        if self.region_type == Region.ADMIN3:
            self.admin3_id, self.admin_3 = self.admin_id, self.admin

    def __getitem__(self, key: str) -> str:
        return getattr(self, key)


def query_country_qnodes(countries: List[str]) -> Dict[str, Optional[str]]:
    # Translates countries to Q-nodes. Returns a dictionary of each input country and its QNode (None if not found)
    # We look for countries in a case-insensitive fashion.
    if not countries:
        return {}

    regions = query_countries(countries)
    result_dict: Dict[str, Optional[str]] = {region.country: region.country_id for region in regions}

    # The result dictionary contains all the countries we have found, we need to add those we did not find
    found_countries = set([country.lower() for country in result_dict.keys()])
    for country in countries:
        if country.lower() not in found_countries:
            result_dict[country] = None

    return result_dict


def list_to_where(field: str, elements: List[str], lower=False) -> Optional[str]:
    if not elements:
        return None

    if lower:
        elements = [element.lower() for element in elements]
        field = f"LOWER({field})"
    santized = [sanitize(element) for element in elements]
    quoted = [f"'{element}'" for element in santized]
    joined = ', '.join(quoted)

    return f"{field} IN ({joined})"


def region_where_clause(region_field: str, region_list: List[str], region_id_field: str,
                        region_id_list: List[str], alias_field: Optional[str] = None) -> str:
    region_where = list_to_where(region_field, region_list, lower=True)
    if alias_field:
        alias_where = list_to_where(alias_field, region_list, lower=True)
    else:
        alias_where = "0 = 1"

    region_id_where = list_to_where(region_id_field, region_id_list)

    if region_where and region_id_where:
        return f'({region_where} OR {region_id_where} OR {alias_where})'
    elif region_where:
        return region_where
    elif region_id_where:
        return region_id_where
    else:
        return "1=1"


def _query_regions(query: str) -> List[Region]:
    dicts = query_to_dicts(query)
    return [Region(**d) for d in dicts]


def query_countries(countries: List[str] = [], country_ids: List[str] = []) -> List[Region]:
    """ Returns a list of countries:
    If countries or country_ids are not empty, only those countries are returned (all of those in both lists)
    Otherwise, all countries are returned
    """
    where = region_where_clause('s_country_label.text', countries, 'e_country.node1', country_ids)
    query = f'''
    SELECT  e_country.node1 AS admin_id,
            s_country_label.text AS admin,
            'Q6256' AS region_type,
            e_country.node1 AS country_id,
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
    WHERE e_country.label='P31' AND e_country.node2='Q6256' AND {where}
    ORDER BY country
    '''

    return _query_regions(query)


def query_admin1s(country: Optional[str] = None, country_id: Optional[str] = None, admin1s: List[str] = [],
                  admin1_ids: List[str] = []) -> List[Region]:
    """
    Returns a list of admin1s. If country or country_id is specified, return the admin1s only of that country.
    If admin1s or admin1_ids are provided, only those admins are returned.
    If all arguments are empty, all admin1s in the system are returned.
    """
    if country and country_id:
        raise ValueError('Only one of country, country_id may be specified')

    if country_id:
        country_where = f"e_country.node2='{country_id}'"
    elif country:  # We are certain country is not None here, but need an `elif` because mypy isn't certain
        country_where = f"LOWER(s_country_label.text)='{country.lower()}'"
    else:
        country_where = "1=1"
    admin1_where = region_where_clause('s_admin1_label.text', admin1s, 'e_admin1.node1', admin1_ids)

    query = f'''
    SELECT  e_admin1.node1 AS admin_id,
            s_admin1_label.text AS admin,
            'Q10864048' AS region_type,
            e_country.node2 AS country_id,
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
    WHERE e_admin1.label='P31' AND e_admin1.node2='Q10864048' AND {country_where} AND {admin1_where}
    ORDER BY admin1
    '''

    return _query_regions(query)


def query_admin2s(admin1: Optional[str] = None, admin1_id: Optional[str] = None, admin2s: List[str] = [],
                  admin2_ids: List[str] = []) -> List[Region]:
    """
    Returns a list of admin2s. If admin1 or admin1_id is specified, return the admin2s only of that admin1.
    If admin2s or admin2_ids are provided, only those admins are returned.
    If all arguments are empty, all admin2s in the system are returned.
    """
    if admin1 and admin1_id:
        raise ValueError('Only one of admin1, admin1_id may be specified')

    if admin1_id:
        admin1_where = f"e_admin1.node2='{admin1_id}'"
    elif admin1:
        admin1_where = f"LOWER(s_admin1_label.text)=LOWER('{admin1}')"
    else:
        admin1_where = "1=1"

    admin2_where = region_where_clause('s_admin2_label.text', admin2s, 'e_admin2.node1', admin2_ids)

    query = f'''
    SELECT  e_admin2.node1 AS admin_id,
            s_admin2_label.text AS admin,
            'Q13220204' AS region_type,
            e_country.node2 AS country_id,
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
    WHERE e_admin2.label='P31' AND e_admin2.node2='Q13220204' AND {admin1_where} AND {admin2_where}
    ORDER BY admin2
    '''

    return _query_regions(query)


def query_admin3s(admin2: Optional[str] = None, admin2_id: Optional[str] = None, admin3s: List[str] = [],
                  admin3_ids: List[str] = [], debug=False) -> List[Region]:
    """
    Returns a list of admin3s. If admin2 or admin2_id is specified, return the admin3s only of that admin2.
    If admin3s or admin3_ids are provided, only those admins are returned.
    If all arguments are empty, all admin3s in the system are returned.
    """
    if admin2 and admin2_id:
        raise ValueError('Only one of admin2, admin2_id may be specified')

    if admin2_id:
        admin2_where = f"e_admin2.node2='{admin2_id}'"
    elif admin2:
        admin2_where = f"LOWER(s_admin2_label.text)=LOWER('{admin2}')"
    else:
        admin2_where = "1=1"

    admin3_where = region_where_clause('s_admin3_label.text', admin3s, 'e_admin3.node1', admin3_ids)

    query = f'''
    SELECT  e_admin3.node1 AS admin_id,
            s_admin3_label.text AS admin,
            'Q13221722' AS region_type,
            e_country.node2 AS country_id,
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
    WHERE e_admin3.label='P31' AND e_admin3.node2='Q13221722' AND {admin2_where} AND {admin3_where}
    ORDER BY admin3
    '''
    if debug:
        print(query)

    return _query_regions(query)


def query_admins(admins: List[str] = [], admin_ids: List[str] = [], debug=False) -> List[Region]:
    where = region_where_clause('s_region_label.text', admins, 'e_region.node1', admin_ids, 's_region_alias.text')

    query = f'''
    SELECT e_region.node1 AS admin_id, s_region_label.text AS admin, e_region.node2 AS region_type,
        e_country.node2 AS country_id, s_country_label.text AS country,
        e_admin1.node2 AS admin1_id, s_admin1_label.text AS admin1,
        e_admin2.node2 AS admin2_id, s_admin2_label.text AS admin2,
        'POINT(' || c_coordinate.longitude || ' ' || c_coordinate.latitude || ')' as region_coordinate
        FROM edges e_region
        JOIN edges e_region_label ON (e_region_label.node1=e_region.node1 AND e_region_label.label='label')
        JOIN strings s_region_label ON (e_region_label.id=s_region_label.edge_id)
        JOIN edges e_country
            JOIN edges e_country_label
                JOIN strings s_country_label
                ON (s_country_label.edge_id=e_country_label.id)
            ON (e_country.node2=e_country_label.node1 AND e_country_label.label='label')
        ON (e_region.node1=e_country.node1 AND e_country.label='P17')
        LEFT JOIN edges e_admin1
            JOIN edges e_admin1_label
                JOIN strings s_admin1_label
                ON (s_admin1_label.edge_id=e_admin1_label.id)
            ON (e_admin1.node2=e_admin1_label.node1 AND e_admin1_label.label='label')
        ON (e_region.node1=e_admin1.node1 AND e_admin1.label='P2006190001')
        LEFT JOIN edges e_admin2
            JOIN edges e_admin2_label
                JOIN strings s_admin2_label
                ON (s_admin2_label.edge_id=e_admin2_label.id)
            ON (e_admin2.node2=e_admin2_label.node1 AND e_admin2_label.label='label')
        ON (e_region.node1=e_admin2.node1 AND e_admin2.label='P2006190002')
        LEFT JOIN edges e_coordinate
            JOIN coordinates c_coordinate
            ON (c_coordinate.edge_id=e_coordinate.id)
        ON (e_region.node1=e_coordinate.node1 AND e_coordinate.label='P625')
        LEFT JOIN edges e_region_alias
            JOIN strings s_region_alias
            ON (s_region_alias.edge_id=e_region_alias.id)
          ON (e_region.node1=e_region_alias.node1 AND e_region_alias.label='alias')
    WHERE e_region.label='P31' AND e_region.node2 IN ('Q6256', 'Q10864048', 'Q13220204', 'Q13221722') AND {where}
    '''
    if debug:
        print(query)
    return _query_regions(query)
