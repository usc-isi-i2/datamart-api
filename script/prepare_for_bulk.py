# This script takes KGTK tsv files, extract edges from them and copies them to one TSV file that
# can be added to Postgres with the COPY FROM command.
#
# It is considerably faster than using `import_tsv.py`.
#
# To import the files into Postgres use the bulk_copy.py script

import csv
import glob
import os
import sys
from argparse import ArgumentParser
from csv import DictReader, writer

import psycopg2
from colorama import Fore, Style

# Allow running from the command line - python script/import... doesn't add the root project directory
# to the PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(__file__)))


from db.sql.kgtk import create_edge_objects, import_kgtk_tsv
from db.sql.models import (CoordinateValue, DateValue, Edge, QuantityValue,
                           StringValue, SymbolValue)
from db.sql.utils import db_connection
from config import DB

def parse_args():
    parser = ArgumentParser()
    parser.add_argument('input', nargs='+', help='Input KGTK files')
    parser.add_argument('--output-dir', type=str,
                        required=True, help='Directory for output TSV file')
    parser.add_argument('--ignore-duplicates', action='store_true', default=False,
                        help='Do not try to avoid duplicate edges')

    return parser.parse_args()

def warn(line, *args):
    print(f"{Fore.YELLOW}Line {line:7}:", *args, Style.RESET_ALL)

def read_input_file(filename, ids):
    print(f'{filename}...')
    count = 0
    errors = 0
    with open(filename, "r", encoding="utf-8") as f:
        reader = DictReader(f, delimiter='\t')
        for row in reader:
            count += 1
            id = row.get('id')
            node1 = row.get('node1')
            label = row.get('label')
            node2 = row.get('node2')

            if not id and not node1:
                warn(count, "Line must contain either id or node1")
                errors += 1
                continue
            if not node1:
                # In some files, node1 is called 'id'
                node1 = id
                id = None
                row['node1'] = node1
            if not id:
                for edge_num in range(1, 100):
                    id = f"{node1}-{label}-{'%02d' % edge_num}"
                    if id not in ids:
                        break
                row['id'] = id

            try:
                edge, value = create_edge_objects(row)
                yield edge, value
            except ValueError:
                warn(count, f"Can't deduce edge type, id is {id}")
                errors += 1
            if count % 100000 == 0:
                print(f'Read {count} records from {filename}')

    print(f'Read {count} records with {errors} errors from {filename}')


class TypeProcessor:
    def __init__(self, type, output_dir, filename):
        self._file = open(os.path.join(output_dir, filename +
                                       ".tsv"), 'w', encoding='utf-8', newline='')
        self.type = type
        self._writer = csv.writer(self._file, delimiter='\t')

    def write(self, obj):
        def sanitize(field):
            # String fields should have \ escaned, and \xa0 (non-break space) replaced with an ordinary space
            if isinstance(field, str):
                return field.replace('\\', '\\\\').replace('\xa0',' ')

            return field

        if type(obj) != self.type:
            raise ValueError('Wrong object type passed to processor')
        row = self.get_row(obj)
        sanitized_row = list(sanitize(field) for field in row)
        self._writer.writerow(sanitized_row)

    def close(self):
        self._file.close()

    def get_row(self, object):
        raise NotImplementedError('Please implement get_row')


class EdgeProcessor(TypeProcessor):
    def __init__(self, output_dir):
        super().__init__(Edge, output_dir, 'edges')

    def get_row(self, object):
        return (object.id, object.node1, object.label, object.node2, object.data_type)


class StringProcessor(TypeProcessor):
    def __init__(self, output_dir):
        super().__init__(StringValue, output_dir, 'strings')

    def get_row(self, object):
        return (object.edge_id, object.text, object.language)


class DateProcessor(TypeProcessor):
    def __init__(self, output_dir):
        super().__init__(DateValue, output_dir, 'dates')

    def get_row(self, object):
        return (object.edge_id, object.date_and_time.isoformat(), object.calendar, object.precision)


class QuantityProcessor(TypeProcessor):
    def __init__(self, output_dir):
        super().__init__(QuantityValue, output_dir, 'quantities')

    def get_row(self, object):
        return (object.edge_id, object.number, object.unit, object.low_tolerance, object.high_tolerance)


class CoordinateProcessor(TypeProcessor):
    def __init__(self, output_dir):
        super().__init__(CoordinateValue, output_dir, 'coordinates')

    def get_row(self, object):
        return (object.edge_id, object.longitude, object.latitude, object.precision)


class SymbolProcessor(TypeProcessor):
    def __init__(self, output_dir):
        super().__init__(SymbolValue, output_dir, 'symbols')

    def get_row(self, object):
        return (object.edge_id, object.symbol)


def create_processors(dir):
    processor_types = [EdgeProcessor, StringProcessor, DateProcessor,
                       QuantityProcessor, CoordinateProcessor, SymbolProcessor]
    processors = {}
    for pt in processor_types:
        processor = pt(dir)
        processors[processor.type.__name__] = processor

    return processors


def close_processors(processors):
    for processor in processors.values():
        processor.close()

def read_existing_edges(args):
    existing = set()

    config_object = { 'DB': DB }
    with db_connection(config_object) as conn:
        print(f'Reading edge IDs from the database')
        with conn.cursor('edges') as cursor:  # Server side cursor
            cursor.itersize = 100000
            query = "SELECT id FROM edges"
            cursor.execute(query)
            for record in cursor:
                existing.add(record[0])
                if len(existing) % 1000000 == 0:
                    print(f'...{len(existing):,}')
    print(f'Retrieved {len(existing)} ids')

    return existing

def run():
    args = parse_args()

    # Make sure duplicate edges are only copied once.
    # Note that we do not compare the data to the database itself. Duplicates will be handled when
    # copying to the database.
    if not args.ignore_duplicates:
        existing = read_existing_edges(args)
    else:
        existing = {}
    new = set ()

    written = 0
    skipped = 0
    print(f'Writing output to directory {args.output_dir}')
    processors = create_processors(args.output_dir)

    for pattern in args.input:
        for filename in glob.glob(pattern):
            for edge, value in read_input_file(filename, new):
                if not args.ignore_duplicates and edge.id in existing:
                    # Edge already exists in database
                    skipped += 1
                    continue
                if edge.id in new:
                    # Edge already processed in this batch
                    skipped += 1
                    continue
                processors[type(edge).__name__].write(edge)
                processors[type(value).__name__].write(value)
                new.add(edge.id)
                written += 1

    print(f'Done, written {written} rows, skipped {skipped}')


if __name__ == '__main__':
    run()
