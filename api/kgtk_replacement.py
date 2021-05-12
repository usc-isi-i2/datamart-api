# import pandas as pd
import hashlib

def build_wikidata_id(row, node1_column_idx, node2_column_idx, label_column_idx, value_hash_width=6, id_separator="-"):
    node2_value = row[node2_column_idx]
    if value_hash_width > 0 and node2_value.startswith(('L', 'P', 'Q')):
        return row[node1_column_idx] + id_separator + row[label_column_idx] + id_separator + row[node2_column_idx]
    else:
        return row[node1_column_idx] + id_separator + row[label_column_idx] + id_separator + \
            hashlib.sha256(node2_value.encode('utf-8')).hexdigest()[:value_hash_width]

def add_ids(df):
    column_names = df.column_names.copy()
    node1_column_idx = df.node1_column_idx
    label_column_idx = df.label_column_idx
    node2_column_idx = df.node2_column_idx
    # id_column_idx = -1 # default

    # id_style='wikidata'
    if node1_column_idx < 0:
        raise ValueError("No node1 column index")
    if label_column_idx < 0:
        raise ValueError("No label column index")
    if node2_column_idx < 0:
        raise ValueError("No node2 column index")

    # if id_column_idx >= 0:
    #     # The input file has an ID column.  Use it.
    #     # old_id_column_name = column_names[id_column_idx]
    #     old_id_column_idx = id_column_idx
    # else:
    #     # There is not old ID column index.
    #     old_id_column_idx = -1
    #     # old_id_column_name = ""


    # # The new ID column was not explicitly named.
    # if id_column_idx >= 0:
    #     # The input file has an ID column.  Use it.
    #     new_id_column_name = column_names[id_column_idx]
    #     new_id_column_idx = id_column_idx
    #     add_new_id_column = False
    # else:
    #     # Create a new ID column.
    #     new_id_column_idx = len(column_names)
    #     new_id_column_name = "id"
    #     column_names.append(new_id_column_name)
    #     add_new_id_column = True

    new_id_column_idx = len(column_names)
    new_id_column_name = "id"

    # claim_id_column_name = "claim_id"
    # # claim_id_column_idx = column_name_map.get(claim_id_column_name, -1)
    # initial_id = -1

    df[new_id_column_name] = None # add a new column for the ids.
    for row in df.iterrows():
        # if add_new_id_column: 
        #     row.append("")
        # elif old_id_column_idx >= 0:
        #     if row[old_id_column_idx] != "":
        #         if new_id_column_idx != old_id_column_idx:
        #             row[new_id_column_idx] = row[old_id_column_idx]
        #         continue
        new_id = build_wikidata_id(row)
        row[new_id_column_idx] = new_id

    return df