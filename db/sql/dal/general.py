# General data queries for SQL
# This file contains a class that implements a lot of SQL queries.
# The idea behind this class is to replace it with an equivalent implementation that performs SPARQL queries.
#
# We need to reorganize this in the future, all queries shouldn't be in the same place

from typing import List, Dict
from db.sql.utils import query_to_dicts
from db.sql import search_views
import re

_sanitation_pattern = re.compile(r'[^\w_\- /#:\.]')


def sanitize(term: str) -> str:
    # Remove all non-alphanumeric or space characters from term
    sanitized = _sanitation_pattern.sub('', term)
    return sanitized


def get_dataset_id(dataset):
    dataset = sanitize(dataset)
    dataset_query = f'''
    SELECT e_dataset.node1 AS dataset_id
        FROM edges e_dataset
		JOIN edges e_p31 ON (e_dataset.node1=e_p31.node1 AND e_p31.label='P31')
    WHERE e_dataset.label='P1813' AND e_dataset.node2='{dataset}' AND e_p31.node2='Q1172284';
    '''
    dataset_dicts = query_to_dicts(dataset_query)
    if len(dataset_dicts) > 0:
        return dataset_dicts[0]['dataset_id']
    return None


def qnode_exists(qnode) -> bool:
    qnode = sanitize(qnode)
    qnode_query = f'''
    SELECT  count(*) AS count FROM edges
    WHERE node1='{qnode}';
    '''
    result = query_to_dicts(qnode_query)
    return result[0]['count'] > 0


def get_label(qnode, default=None, lang='en'):
    qnode = sanitize(qnode)
    lang = sanitize(lang)

    label_query = f'''
    SELECT node1 as qnode, text as label
    FROM edges e
    INNER JOIN strings s on e.id = s.edge_id
    WHERE e.node1 = '{qnode}' and e.label = 'label' and s.language='{lang}';
    '''
    label = query_to_dicts(label_query)
    if len(label) > 0:
        return label[0]['label']
    return default


def next_variable_value(dataset_id, prefix) -> int:
    dataset_id = sanitize(dataset_id)

    query = f'''
    select max(substring(e_variable.node2 from '{prefix}#"[0-9]+#"' for '#')::INTEGER)  from edges e_variable
    where e_variable.node1 in
(
    select e_dataset.node2 from edges e_dataset
    where e_dataset.node1 = '{dataset_id}'
    and e_dataset.label = 'P2006020003'
)
and e_variable.label = 'P1813' and e_variable.node2 similar to '{prefix}[0-9]+';
    '''
    result = query_to_dicts(query)
    if len(result) > 0 and result[0]['max'] is not None:
        number = result[0]['max'] + 1
    else:
        number = 0
    return number


def fuzzy_query_variables(questions: List[str], regions: Dict[str, List[str]], tags: List[str], limit: int, debug=False):
    def get_region_where():
        # Adds the where clause for regions specified in the regions dict
        # We have two EXIST clauses per admin type - one for variables whose main_subject is the location,
        # and one for variables whose location is a qualifier.
        admin_wheres = []
        for admin, qnodes in regions.items():
            if not qnodes:
                continue
            qnode_list = ', '.join([f"'{qnode}'" for qnode in qnodes])
            view_name = search_views.get_view_name(admin)
            one_where = f"EXISTS (SELECT 1 FROM {view_name} WHERE {view_name}.variable_id=e_var_name.node2 AND {view_name}.dataset_qnode=e_dataset.node1 AND {view_name}.{admin}_qnode IN ({qnode_list}))"
            admin_wheres.append(one_where)

        if not admin_wheres:
            return "1=1"
        return '\nOR '.join(admin_wheres)

    def get_tag_where():
        if not tags:
            return "1=1"

        tag_list = ', '.join([f"'{tag}'" for tag in tags])
        return f"s_tag.text IN ({tag_list})"

    if not questions:
        return no_keywords_query_variables(get_region_where(), get_tag_where(), limit, debug)

    if debug:
        print('questions:', questions)
    sanitized = [sanitize(question) for question in questions]
    ts_queries = [f"plainto_tsquery('{question}')" for question in sanitized]
    if debug:
        print('ts_queries', ts_queries)
    combined_ts_query = '(' + ' || '.join(ts_queries) + ')'
    if debug:
        print('combined_ts_query:', combined_ts_query)

    region_where = get_region_where()
    tag_where = get_tag_where()

    # Use Postgres's full text search capabilities
    sql = f"""
    SELECT fuzzy.variable_id, fuzzy.variable_qnode, fuzzy.dataset_qnode, fuzzy.name,  ts_rank(variable_text, {combined_ts_query}) AS rank FROM
        (SELECT e_var_name.node2 AS variable_id,
                e_var_name.node1 AS variable_qnode,
                -- e_dataset_name.node2 AS dataset_id,
                e_dataset.node1 AS dataset_qnode,
                to_tsvector(CONCAT(s_description.text, ' ', s_name.text, ' ', s_label.text)) AS variable_text,
                CONCAT(s_name.text, ' ', s_label.text) as name
            FROM edges e_var
            JOIN edges e_var_name ON (e_var_name.node1=e_var.node1 AND e_var_name.label='P1813')
            JOIN edges e_dataset ON (e_dataset.label='P2006020003' AND e_dataset.node2=e_var.node1)
                    -- JOIN edges e_dataset_name ON (e_dataset_name.node1=e_dataset.node1 AND e_dataset_name.label='P1813')
            LEFT JOIN edges e_description JOIN strings s_description ON (e_description.id=s_description.edge_id) ON (e_var.node1=e_description.node1 AND e_description.label='description')
            LEFT JOIN edges e_name JOIN strings s_name ON (e_name.id=s_name.edge_id) ON (e_var.node1=e_name.node1 AND e_name.label='P1813')
            LEFT JOIN edges e_label JOIN strings s_label ON (e_label.id=s_label.edge_id) ON (e_var.node1=e_label.node1 AND e_label.label='label')
            LEFT JOIN edges e_tag JOIN strings s_tag ON (e_tag.id=s_tag.edge_id) ON (e_var.node1=e_tag.node1 AND e_tag.label='P2010050001')

        WHERE e_var.label='P31' AND e_var.node2='Q50701' AND ({region_where}) AND ({tag_where})) AS fuzzy
    WHERE variable_text @@ {combined_ts_query}
    ORDER BY rank DESC
    LIMIT {limit}
    """
    if debug:
        print(sql)
    results = query_to_dicts(sql)

    return results


def no_keywords_query_variables(region_where: str, tag_where: str, limit: int, debug=False):
    sql = f"""
    SELECT e_var_name.node2 AS variable_id,
           e_var_name.node1 AS variable_qnode,
                -- e_dataset_name.node2 AS dataset_id,
                e_dataset.node1 AS dataset_qnode,
                to_tsvector(CONCAT(s_description.text, ' ', s_name.text, ' ', s_label.text)) AS variable_text,
                CONCAT(s_name.text, ' ', s_label.text) as name
            FROM edges e_var
            JOIN edges e_var_name ON (e_var_name.node1=e_var.node1 AND e_var_name.label='P1813')
            JOIN edges e_dataset ON (e_dataset.label='P2006020003' AND e_dataset.node2=e_var.node1)
                    -- JOIN edges e_dataset_name ON (e_dataset_name.node1=e_dataset.node1 AND e_dataset_name.label='P1813')
            LEFT JOIN edges e_description JOIN strings s_description ON (e_description.id=s_description.edge_id) ON (e_var.node1=e_description.node1 AND e_description.label='description')
            LEFT JOIN edges e_name JOIN strings s_name ON (e_name.id=s_name.edge_id) ON (e_var.node1=e_name.node1 AND e_name.label='P1813')
            LEFT JOIN edges e_label JOIN strings s_label ON (e_label.id=s_label.edge_id) ON (e_var.node1=e_label.node1 AND e_label.label='label')
            LEFT JOIN edges e_tag JOIN strings s_tag ON (e_tag.id=s_tag.edge_id) ON (e_var.node1=e_tag.node1 AND e_tag.label='P2010050001')

        WHERE e_var.label='P31' AND e_var.node2='Q50701' AND ({region_where}) AND ({tag_where})
        LIMIT {limit}
    """
    if debug:
        print(sql)
    results = query_to_dicts(sql)

    return results
