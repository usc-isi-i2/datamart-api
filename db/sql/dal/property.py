from collections.abc import Iterable
from db.sql.dal.general import sanitize
from db.sql.utils import query_edges_to_df, query_to_dicts, delete
from pandas import DataFrame

# Double entries
# wikidata=# select node1, count(*) as count from edges where node2 = 'Q18616576'  group by node1 having count(*)>1 limit 10;
#  P200612-World-Bank-Id |     2


def query_property(property_name: str = None) -> DataFrame:
    if property_name is None:
        where_clause = "where node1 like 'P%'"
    else:
        where_clause = f"where node1 = '{sanitize(property_name)}'"
    return query_edges_to_df(where_clause)

def check_existing_properties(properties: Iterable) -> list:
    property_list = ','.join([f"'{sanitize(name)}'" for name in properties])
    sql = f'select distinct node1 from edges where node1 in ({property_list})'
    result = [d['node1'] for d in query_to_dicts(sql)]
    return result

def is_property_used(property_name: str) -> bool:
    sql = f"select count(*) as count from edges where label = '{sanitize(property_name)}'"
    result = query_to_dicts(sql)
    return result[0]['count'] > 0

def delete_property(property_name: str = None):
    sql = f"delete from edges where node1='{sanitize(property_name)}'"
    delete(sql)
