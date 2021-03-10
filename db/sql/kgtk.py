# This file contains code that imports a KGTK file into the database. This code is taken from the postgres-wikidata
# repository, and should at some point be united into the KGTK toolkit

import csv
# Import edges from a KGTK TSV file
import datetime
import os.path
import shutil
import subprocess
import tempfile
import time
from csv import DictReader
from typing import Tuple, List, Dict

import dateutil.parser

from db.sql.models import (CoordinateValue, DateValue, Edge, QuantityValue,
                           StringValue, SymbolValue)
from db.sql.utils import create_sqlalchemy_session, postgres_connection


def create_edge_objects(row):
    # Returns a tuple of a edge and an additional Value object (or None if there is no such object)

    def get_edge_object(row):
        edge = Edge(id=row['id'],
                    node1=row['node1'],
                    label=row['label'],
                    node2=row['node2'],
                    data_type=row['node2;kgtk:data_type'])  # rank is optional
        return edge

    def get_value_object(row):
        # Try the value types one by one, until return the correct one (if any)
        type_funcs = [get_date_object, get_coordinate_object,
                      get_quantity_object, get_symbol_object, get_string_object]
        for func in type_funcs:
            obj = func(row)
            if obj:
                return obj

        raise ValueError(f'Row has no value object', row)

    def get_date_object(row):
        # node2;magnitude should be of a caret followed by an ISO date. node2;calendar and node2;precision are optional

        date = row.get('node2;kgtk:date_and_time')
        if not date:
            return None
        try:
            date = dateutil.parser.isoparse(date)
        except ValueError:
            return None

        calendar = row.get('node2;kgtk:calendar')
        precision = row.get('node2;kgtk:precision')
        obj = DateValue(edge_id=row['id'],
                        date_and_time=date,
                        calendar=calendar or None,
                        precision=precision or None)

        return obj

    def get_coordinate_object(row):
        longitude = row.get('node2;kgtk:longitude')
        latitude = row.get('node2;kgtk:latitude')
        precision = row.get('node2;precision')
        if not longitude and not latitude:
            return None

        try:
            longitude = float(longitude)
            latitude = float(latitude)
        except ValueError:
            raise ValueError('Long or lat not numeric', row)

        return CoordinateValue(edge_id=row['id'], longitude=longitude, latitude=latitude, precision=precision)

    def get_quantity_object(row):
        number = row.get('node2;kgtk:number')
        if not number:
            return None
        try:
            number = float(number)
        except ValueError:
            return None

        high_tolerance = row.get('node2;kgtk:high_tolerance') or None
        low_tolerance = row.get('node2;kgtk:low_tolerance') or None
        try:
            if high_tolerance:
                high_tolerance = float(high_tolerance)
            if low_tolerance:
                low_tolerance = float(low_tolerance)
        except ValueError:
            raise ValueError('High or low tolerance not numeric', row)

        unit = row.get('node2;kgtk:units_node')
        return QuantityValue(edge_id=row['id'],
                             number=number, high_tolerance=high_tolerance, low_tolerance=low_tolerance, unit=unit)

    def get_symbol_object(row):
        symbol = row['node2;kgtk:symbol']
        if symbol == '' or symbol is None:
            return None
        return SymbolValue(edge_id=row['id'], symbol=symbol)

    def get_string_object(row):
        data_type = row.get('node2;kgtk:data_type')
        text = row.get('node2;kgtk:text')
        language = row.get('node2;kgtk:language')

        if data_type != 'string' and not text:  # do not rely on data_type, but if it says string, accept empty strings as well
            return None
        if not language:
            language = None

        return StringValue(edge_id=row['id'], text=text, language=language)

    edge = get_edge_object(row)
    value = get_value_object(row)

    return edge, value


def unquote(string):
    if len(string)>1 and string[0] == '"' and string[-1] == '"':
        # Kgtk escapes double quotes, remove the escape characters
        return string[1:-1].replace('\\"', '"')
    else:
        return string

def unquote_dict(row: dict):
    for key, value in row.items():
        row[key] = unquote(value)

def import_kgtk_tsv(filename: str, config=None, delete=False, replace=False, conn=None):
    def column_names(fields):
        for field in fields:
            if field[-2:] == '$?':
                yield field[:-2]
            elif field[-1] in ('$', '?'):
                yield field[:-1]
            else:
                yield field

    def object_values(obj, fields, column_names):
        def format_value(obj, field, column):
            val = getattr(obj, column, None)
            if val is None:
                if not '?' in field:
                    raise ValueError(f"Non nullable field {column} as a null value")
                return 'NULL'
            val = str(val).replace("'", "''")
            if '$' in field:
                return f"'{val}'"
            return val

        values = []
        for (idx, field) in enumerate(fields):
            column = column_names[idx]
            values.append(format_value(obj, field, column))
        return values

    def formatted_object_values(obj, fields, column_names):
        values = object_values(obj, fields, column_names)
        return "(" + ", ".join(values) + ")"

    # Map from object type name to ('table-name', list of fields)
    # A $ signifies a string value. A ? signifies a nullable value
    OBJECT_INFO = {
        'Edge': ('edges', ['id$', 'node1$', 'label$', 'node2$', 'data_type$']),
        'StringValue': ('strings', ['edge_id$', 'text$', 'language$?']),
        'DateValue': ('dates', ['edge_id$', 'date_and_time$', 'precision$?', 'calendar$?']),
        'QuantityValue': ('quantities', ['edge_id$', 'number', 'unit$?', 'low_tolerance?', 'high_tolerance?']),
        'CoordinateValue': ('coordinates', ['edge_id$', 'latitude', 'longitude', 'precision$?']),
        'SymbolValue': ('symbols', ['edge_id$', 'symbol$']),
    }

    def write_objects(typename, objects):
        nonlocal OBJECT_INFO

        table_name, fields = OBJECT_INFO[typename]
        columns = list(column_names(fields))

        CHUNK_SIZE = 10000
        for x in range(0, len(objects), CHUNK_SIZE):
            statement = f"INSERT INTO {table_name} ( {', '.join(columns)} ) VALUES\n"
            slice = objects[x:x+CHUNK_SIZE]
            values = [formatted_object_values(obj, fields, columns) for obj in slice]
            statement += ',\n'.join(values)
            statement += "\nON CONFLICT DO NOTHING;"
            cursor.execute(statement)

    def save_objects(type_name: str, objects: List[Tuple]):
        edges = [t[0] for t in objects]
        write_objects('Edge', edges)
        values = [t[1] for t in objects]
        write_objects(type_name, values)

    def delete_object_records(typename, objects):
        nonlocal OBJECT_INFO
        table_name, fields = OBJECT_INFO[typename]
        columns = list(column_names(fields))

        # The id is the first column
        CHUNK_SIZE = 10000 # Delete 10000 rows at a time
        for x in range(0, len(objects), CHUNK_SIZE):
            slice = objects[x:x+CHUNK_SIZE]
            ids = [object_values(obj, fields[:1], columns[:1])[0] for obj in slice]
            ids_string = '(' + ','.join(ids) + ')'
            statement = f"DELETE FROM {table_name} WHERE {columns[0]} IN {ids_string};"
            cursor.execute(statement)

    def delete_objects(type_name: str, objects: List[Tuple]):
        edges = [t[0] for t in objects]
        delete_object_records('Edge', edges)
        values = [t[1] for t in objects]
        delete_object_records(type_name, values)


    if delete and replace:
        raise ValueError("delete and replace can't both be True")

    obj_map: Dict[str, List[Tuple]] = dict()   # Map from value type to list of (edge, value)
    start = time.time()
    print("Reading rows")
    with open(filename, "r", encoding="utf-8") as f:
        reader = DictReader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
        row_num = 1
        for row in reader:
            row_num += 1
            unquote_dict(row)
            try:
                edge, value = create_edge_objects(row)
            except:
                print(f"Error in row {row_num}")
                raise
            value_type = type(value).__name__
            if value_type not in obj_map:
                obj_map[value_type] = []
            obj_map[value_type].append((edge, value))

    count = 0
    for (type_name, objects) in obj_map.items():
        count += len(objects)
        print(f"{type_name}\t{len(objects)}")
    print(f"Read {count} objects in {time.time() - start}")

    if count == 0:
        return

    # Time to write the edges
    if config and not 'POSTGRES' in config:
        config = dict(POSTGRES=config)

    try:
        if not conn:
            conn = postgres_connection(config)
            our_conn = True
        else:
            our_conn = False
        with conn.cursor() as cursor:
            # Everything here runs under one transaction
            for (type_name, objects) in obj_map.items():
                if delete or replace:
                    delete_objects(type_name, objects)
                    print(f"Deleted {len(objects)} of {type_name} - {time.time() - start}")
                if not delete:
                    save_objects(type_name, objects)
                    print(f"Saved {len(objects)} of {type_name} - {time.time() - start}")

        if our_conn:
            conn.commit()
    finally:
        if our_conn:
            our_conn.close()

    print(f"Done saving {count} objects in {time.time() - start}")

    return

def import_kgtk_dataframe(df, config=None, is_file_exploded=False, conn=None):
    temp_dir = tempfile.mkdtemp()
    try:
        tsv_path = os.path.join(temp_dir, f'kgtk.tsv')
        exploded_tsv_path = os.path.join(temp_dir, f'kgtk-exploded.tsv')

        df.to_csv(tsv_path, sep='\t', index=False, quoting=csv.QUOTE_NONE, quotechar='')

        if not is_file_exploded:
            subprocess.run(['kgtk', 'explode', "-i", tsv_path, '-o', exploded_tsv_path, '--allow-lax-qnodes'])
            if not os.path.isfile(exploded_tsv_path):
                raise ValueError("Couldn't create exploded TSV file")

            import_kgtk_tsv(exploded_tsv_path, config, conn=conn)
        else:
            import_kgtk_tsv(tsv_path, config=config, conn=conn)
    finally:
        shutil.rmtree(temp_dir)
