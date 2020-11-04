from typing import Dict, List, Optional
from db.sql.dal.general import sanitize
from db.sql.utils import query_to_dicts


class Region:
    admin: str
    admin_id: str
    region_type: str
    country: str
    country_id: str
    country_cameo: Optional[str]
    admin1: Optional[str]
    admin1_id: Optional[str]
    admin2: Optional[str]
    admin2_id: Optional[str]
    admin3: Optional[str]
    admin3_id: Optional[str]
    region_coordinate: Optional[str]
    alias: Optional[str]

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
        self.alias = kwargs.get('alias')
        self.country_cameo = kwargs.get('country_cameo')

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
    if not region_list and not region_id_list:
        return "1=1"

    region_where = list_to_where(region_field, region_list, lower=True) or "0=1"
    if alias_field:
        alias_where = list_to_where(alias_field, region_list, lower=True) or "0=1"
    else:
        alias_where = "0=1"

    region_id_where = list_to_where(region_id_field, region_id_list) or "0=1"

    return f'({region_where} OR {region_id_where} OR {alias_where})'


def _query_regions(query: str) -> List[Region]:
    dicts = query_to_dicts(query)
    return [Region(**d) for d in dicts]


def query_admins(admins: List[str] = [], admin_ids: List[str] = [], debug=False) -> List[Region]:
    where = region_where_clause('s_region_label.text', admins, 'e_region.node1', admin_ids, 's_region_alias.text')

    query = f'''
    SELECT e_region.node1 AS admin_id, s_region_label.text AS admin, e_region.node2 AS region_type,
        e_country.node2 AS country_id, s_country_label.text AS country, s_country_cameo.text AS country_cameo,
        e_admin1.node2 AS admin1_id, s_admin1_label.text AS admin1,
        e_admin2.node2 AS admin2_id, s_admin2_label.text AS admin2,
        'POINT(' || c_coordinate.longitude || ' ' || c_coordinate.latitude || ')' as region_coordinate,
        s_region_alias.text AS alias
        FROM edges e_region
        JOIN edges e_region_label ON (e_region_label.node1=e_region.node1 AND e_region_label.label='label')
        JOIN strings s_region_label ON (e_region_label.id=s_region_label.edge_id)
        JOIN edges e_country
            JOIN edges e_country_label
                JOIN strings s_country_label
                ON (s_country_label.edge_id=e_country_label.id)
            ON (e_country.node2=e_country_label.node1 AND e_country_label.label='label')
            LEFT JOIN edges e_country_cameo
                JOIN strings s_country_cameo
                ON (s_country_cameo.edge_id=e_country_cameo.id)
            ON (e_country.node2=e_country_cameo.node1 AND e_country_cameo.label='P2010270001')
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
