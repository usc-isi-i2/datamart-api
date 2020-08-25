# This file contains code that imports a KGTK file into the database. This code is taken from the postgres-wikidata
# repository, and should at some point be united into the KGTK toolkit

# Import edges from a KGTK TSV file
import datetime
from csv import DictReader
import csv

from db.sql.models import CoordinateValue, DateValue, SymbolValue, QuantityValue, Edge, StringValue
import dateutil.parser
from db.sql.utils import create_sqlalchemy_session
import tempfile
import shutil
import os.path
import subprocess


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


def import_kgtk_tsv(filename: str, config=None):
    session = create_sqlalchemy_session(config)

    with open(filename, "r", encoding="utf-8") as f:
        reader = DictReader(f, delimiter='\t')

        values = []
        edges = []
        for row in reader:
            edge, value = create_edge_objects(row)
            edges.append(edge)
            values.append(value)

    # Working in chunks is a lot faster than feeding everything to the database at once.
    CHUNK_SIZE = 50000
    for start in range(0, len(edges), CHUNK_SIZE):
        edge_chunk = edges[start:start + CHUNK_SIZE]
        value_chunk = values[start:start + CHUNK_SIZE]
        ids = [edge.id for edge in edge_chunk]
        delete_q = Edge.__table__.delete().where(Edge.id.in_(ids))
        # We have ON DELETE CASCADE on foreign keys, so values are also deleted
        session.execute(delete_q)
        session.bulk_save_objects(edge_chunk)
        session.bulk_save_objects(value_chunk)

    session.commit()


def import_kgtk_dataframe(df, config=None, is_file_exploded=False):
    temp_dir = tempfile.mkdtemp()
    try:
        tsv_path = os.path.join(temp_dir, f'kgtk.tsv')
        exploded_tsv_path = os.path.join(temp_dir, f'kgtk-exploded.tsv')

        df.to_csv(tsv_path, sep='\t', index=False, quoting=csv.QUOTE_NONE)

        if not is_file_exploded:
            subprocess.run(['kgtk', 'explode', tsv_path, '-o', exploded_tsv_path, '--allow-lax-qnodes'])
            if not os.path.isfile(exploded_tsv_path):
                raise ValueError("Couldn't create exploded TSV file")

            import_kgtk_tsv(exploded_tsv_path, config)
        else:
            import_kgtk_tsv(tsv_path, config=config)
    finally:
        shutil.rmtree(temp_dir)
