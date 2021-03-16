from collections.abc import Iterable
from db.sql.dal.general import sanitize
from db.sql.utils import query_edges_to_df, query_to_dicts, delete
from pandas import DataFrame

def query_entity(entity_name: str = None, entity_label: str = None) -> DataFrame:
    if entity_name is None and entity_label is None:
        where_clause = "where node1 like 'Q%' and label = 'label'"
    elif entity_name is None:
        where_clause = f"where node2 like '%{sanitize(entity_label)}%'"
    else:
        where_clause = f"where node1 = '{sanitize(entity_name)}'"
    return query_edges_to_df(where_clause)

def check_existing_entities(entities: Iterable) -> list:
    entity_list = ','.join([f"'{sanitize(name)}'" for name in entities])
    sql = f'select distinct node1 from edges where node1 in ({entity_list})'
    print(sql)
    # print(sql)
    result = [d['node1'] for d in query_to_dicts(sql)]
    # print(query_edges_to_df(f'where node1 in ({entity_list})'))
    return result

def is_entity_used(entity_name: str) -> bool:
    sql = f"select count(*) as count from edges where label <> 'label' and node1 = '{sanitize(entity_name)}' \
            union \
            select count(*) as count from edges where label <> 'label' and node2 = '{sanitize(entity_name)}' \
           "
    result = query_to_dicts(sql)
    return result[0]['count'] > 0

def delete_entity(entity_name: str = None, conn=None):
    sql = f"delete from edges where node1='{sanitize(entity_name)}'"
    delete(sql, conn=conn)
