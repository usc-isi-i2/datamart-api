
from db.sql.dal.general import sanitize
from db.sql.utils import query_to_dicts

def query_country_qnodes(countries):
    # Translates countries to Q-nodes. Returns a dictionary of each input country and its QNode (None if not found)
    # We look for countries in a case-insensitive fashion.
    if not countries:
        return {}

    lower_countries = [sanitize(country).lower() for country in countries]
    quoted_countries = [f"'{country}'" for country in lower_countries]
    countries_in = ', '.join(quoted_countries)

    query = f'''
        SELECT e_country.node1 as qnode, s_country_label.text AS country
            FROM edges e_country
            JOIN edges e_country_label ON (e_country_label.node1=e_country.node1 AND e_country_label.label='label')
            JOIN strings s_country_label ON (e_country_label.id=s_country_label.edge_id)
        WHERE e_country.label='P31' AND e_country.node2='Q6256' AND LOWER(s_country_label.text) IN ({countries_in})
    ''';
    rows = query_to_dicts(query)

    result_dict = { row['country']: row['qnode'] for row in rows }

    # The result dictionary contains all the countries we have found, we need to add those we did not find
    found_countries = set([country.lower() for country in result_dict.keys()])
    for country in countries:
        if country.lower() not in found_countries:
            result_dict[country] = None

    print(result_dict)
    return result_dict

