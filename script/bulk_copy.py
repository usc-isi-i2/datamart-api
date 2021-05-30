import sys
from argparse import ArgumentParser
import os
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from db.sql.utils import postgres_connection
from config import DB

def parse_args():
    parser = ArgumentParser()
    parser.add_argument('input_dir', help='Input directory (accessible from Postgres)')

    args = parser.parse_args()
    return args

def import_table(cursor, base_dir, name, fields):
    def parse_fields(fields):
        # If a field is numeric and nullable, we need to add FORCE_NULL to that field (otherwise empty fields are saved as strings,
        # which causes the operation to fail).
        field_list = fields.split(',')
        field_list = [field.strip() for field in field_list]  # No spaces
        nullable = [field[:-1] for field in field_list if field.endswith('?')] # Nullable fields
        clean = [field.replace('?', '') for field in field_list]

        if nullable:
            # Nullable fields (numerics) may be encoded as empty strings. The FORCE_NULL option turns them into NULLs.
            # FORCE_NULL only works with CSV files, so we specify the CSV format with \t as a delimiter.
            # The E'\t' is PostgreSQL's way of specify \t as a delimiter, this is not a typo
            nullable = ', '.join(nullable)
            force_null = f"WITH (FORMAT(CSV), DELIMITER (E'\t'), FORCE_NULL ({nullable}))"
        else:
            force_null = ''
        fields = ', '. join(clean)

        return fields, force_null

    def get_table_name(entity_name):
        # We may require more sophisticated mapping between entity name and table name
        return entity_name

    table_name = get_table_name(name)
    fields, force_null = parse_fields(fields)
    print(f'Importing table {name} from {base_dir}/{name}.tsv')

    sql = f"""
    COPY {table_name} ({fields}) FROM '{base_dir}/{name}.tsv' {force_null};
    """
    print(sql)
    cursor.execute(sql)

def disable_indices(cursor):
    sql = """
    DROP INDEX ix_edges_label_node2;
    DROP INDEX ix_edges_node1_label;
    DROP INDEX ix_edges_node2;
    DROP INDEX ix_symbols_symbol;
    SET CONSTRAINTS ALL DEFERRED;
    """
    print('Disabling indices and constraints')
    print(sql)
    cursor.execute(sql)

def rebuild_indices(cursor):
    sql = """
        CREATE INDEX ix_edges_label_node2 ON edges(label, node2);
        CREATE INDEX ix_edges_node1_label ON edges(node1, label);
        CREATE INDEX ix_edges_node2 ON edges(node2);
        CREATE INDEX ix_symbols_symbol ON symbols(symbol);
    """
    print('Recreating indices')
    print(sql)
    cursor.execute(sql)


def main():
    args = parse_args()
    satellites = dict(
        edges = 'id, node1, label, node2, data_type',
        quantities = 'edge_id, number, unit, low_tolerance?, high_tolerance?',  # Question mark signifies nullable numeric fields
        strings = 'edge_id, text, language',
        dates = 'edge_id, date_and_time, calendar, precision',
        coordinates = 'edge_id, longitude, latitude, precision',
        symbols = 'edge_id, symbol',
    )
    config_object = { 'DB': DB }
    with postgres_connection(config_object) as conn:
        with conn.cursor() as cursor:
            disable_indices(cursor)
            import_table(cursor, args.input_dir, 'edges', satellites['edges'])
            for satellite, fields in satellites.items():
                if satellite == 'edges':
                    continue
                import_table(cursor, args.input_dir, satellite, fields)
            print('Committing changes')
            conn.commit()

            # Rebuild indices after committing, since in some cases Postgres complains that an object is already in use
            rebuild_indices(cursor)


    print('Done')

if __name__ == '__main__':
    main()
