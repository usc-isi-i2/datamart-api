# This script performs several consistency checks on the Datamart database, and reports problems.

from typing import List
import colorama
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import DB, STORAGE_BACKEND
from db.sql.utils import db_connection

def check_double_labels(conn):
    def query_prefix(prefix: str) -> List[str]:
        if prefix not in ('P', 'Q'):
            raise ValueError('prefix must be P or Q')

        query = f"""
        SELECT node1, COUNT(node2)
            FROM edges
            WHERE node1 LIKE '{prefix}%' AND label='label'
            GROUP BY node1
            HAVING COUNT(node2) > 1
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
            result = [row[0] for row in cursor]

        return result

    print('Look for nodes with double labels')
    qnodes = query_prefix('Q')
    if len(qnodes):
        subject = 'qnodes' if len(qnodes) != 1 else 'qnode'
        print(colorama.Fore.YELLOW, f"{len(qnodes)} {subject} with more than one label: ", ', '. join(qnodes))
    else:
        print(colorama.Fore.LIGHTGREEN_EX, "No Qnodes with multiple labels")

    pnodes = query_prefix('P')
    if len(pnodes):
        subject = 'pnodes' if len(pnodes) != 1 else 'pnode'
        print(colorama.Fore.YELLOW, f"{len(pnodes)} {subject} with more than one label: ", ', '. join(pnodes))
    else:
        print(colorama.Fore.LIGHTGREEN_EX, "No Pnodes with multiple labels")
    print(colorama.Fore.WHITE)

def check_no_label(conn):
    def query_prefix(prefix: str) -> List[str]:
        if prefix not in ('P', 'Q'):
            raise ValueError('prefix must be P or Q')

        query = f"""
        SELECT DISTINCT node1
            FROM edges e1 
            WHERE node1 SIMILAR TO '{prefix}\d+' AND NOT EXISTS (SELECT * FROM edges e2 WHERE e1.node1=e2.node1 AND e2.label='label')
        ORDER BY node1
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
            result = [row[0] for row in cursor]

        return result

    print('Look for nodes with no labels')
    qnodes = query_prefix('Q')
    if len(qnodes):
        subject = 'qnodes' if len(qnodes) != 1 else 'qnode'
        print(colorama.Fore.YELLOW, f"{len(qnodes)} {subject} with no label: ", ', '. join(qnodes))
    else:
        print(colorama.Fore.LIGHTGREEN_EX, "No Qnodes without a label")

    pnodes = query_prefix('P')
    if len(pnodes):
        subject = 'pnodes' if len(pnodes) != 1 else 'pnode'
        print(colorama.Fore.YELLOW, f"{len(pnodes)} {subject} with no label: ", ', '. join(pnodes))
    else:
        print(colorama.Fore.LIGHTGREEN_EX, "No Pnodes without a label")
    print(colorama.Fore.WHITE)

def run():
    colorama.init()
    print('Performing consistency checks on the Datamart Database')
    print()

    with db_connection(config={'DB': DB, 'STORAGE_BACKEND': STORAGE_BACKEND}) as conn:
        with conn.cursor() as cursor:
            check_double_labels(conn)
            check_no_label(conn)

if __name__ == '__main__':
    run()