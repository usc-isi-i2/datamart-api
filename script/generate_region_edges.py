import argparse
import csv
import hashlib
import sys

# Q10864048	first-level administrative country subdivision
# Q13220204	second-level administrative country subdivision
# Q13221722	third-level administrative country subdivision
# P17 country
# P2006190001	label	"located in the first-level administrative country subdivision"
# P2006190002	label	"located in the second-level administrative country subdivision"
# P2006190003	label	"located in the third-level administrative country subdivision"

def print_triple(outfile, node1, label, node2):
    id = hashlib.sha256(bytes('{}{}{}'.format(node1, label, node2), encoding='utf-8')).hexdigest()
    print(f'{node1}\t{label}\t{node2}\t{id}', file=outfile)


def generate_edges(infile, outfile):
    # Expecting header: country,country_id,admin1,admin1_id,admin2,admin2_id,admin3,admin3_id
    generated = set()
    edges = []
    reader = csv.DictReader(infile)
    print(f'node1\tlabel\tnode2\tid', file=outfile)
    for row in reader:

        if row['admin1_id'] and not row['admin1_id'] in generated:
            generated.add(row['admin1_id'])
            print_triple(outfile, row['admin1_id'], 'P31', 'Q10864048')  # instance of
            print_triple(outfile, row['admin1_id'], 'label', row['admin1'])
            print_triple(outfile, row['admin1_id'], 'P17', row['country_id'])  # in country
            print_triple(outfile, row['admin1_id'], 'P2006190001', row['admin1_id'])  # in admin1 (itself)

        if row['admin2_id'] and not row['admin2_id'] in generated:
            generated.add(row['admin2_id'])
            print_triple(outfile, row['admin2_id'], 'P31', 'Q13220204')  # instance of
            print_triple(outfile, row['admin2_id'], 'label', row['admin2'])
            print_triple(outfile, row['admin2_id'], 'P17', row['country_id'])  # in country
            print_triple(outfile, row['admin2_id'], 'P2006190001', row['admin1_id'])  # in admin1
            print_triple(outfile, row['admin2_id'], 'P2006190002', row['admin2_id'])  # in admin2 (itself)

        if row['admin3_id'] and not row['admin3_id'] in generated:
            generated.add(row['admin3_id'])
            print_triple(outfile, row['admin3_id'], 'P31', 'Q13221722')  # instance of
            print_triple(outfile, row['admin3_id'], 'label', row['admin3'])
            print_triple(outfile, row['admin3_id'], 'P17', row['country_id'])  # in country
            print_triple(outfile, row['admin3_id'], 'P2006190001', row['admin1_id'])  # in admin1
            print_triple(outfile, row['admin3_id'], 'P2006190002', row['admin2_id'])  # in admin2
            print_triple(outfile, row['admin3_id'], 'P2006190003', row['admin3_id'])  # in admin3 (itself)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate localed-in edges based on content region.csv (default from stdin)')
    parser.add_argument("input_file", nargs='?', type=argparse.FileType('rt', encoding='utf-8'), default=sys.stdin,
                        help="input region csv file")
    parser.add_argument("output_file", nargs='?', type=argparse.FileType('wt', encoding='utf-8'), default=sys.stdout,
                        help="output region edge file")

    args = parser.parse_args()
    generate_edges(args.input_file, args.output_file)
