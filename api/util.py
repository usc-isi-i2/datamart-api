import csv
import pandas as pd
from api import kgtk_replacement

from flask_restful import request

class DataInterval:
    name_qnode_map = {
        'millennium': 'Q36507',
        'century': 'Q578',
        'decade': 'Q39911',
        'year': 'Q577',
        'month': 'Q5151',
        'day': 'Q573',
        'hour': 'Q25235',
        'minute': 'Q7727',
        'second': 'Q11574'
    }

    int_name_map = {
        6: 'millennium',
        7: 'century',
        8: 'decade',
        9: 'year',
        10: 'month',
        11: 'day',
        12: 'hour',
        13: 'minute',
        14: 'second'
    }

    int_qnode_map = {
        6: 'Q36507',  # millennium
        7: 'Q578',    # century
        8: 'Q39911',  # decade
        9: 'Q577',    # year
        10: 'Q5151',  # month
        11: 'Q573',   # day
        12: 'Q25235', # hour
        13: 'Q7727',  # minute
        14: 'Q11574'  # second
    }

    name_int_map = {name: value for value, name in int_name_map.items()}
    qnode_name_map = {qnode: name for name, qnode in name_qnode_map.items()}

    @classmethod
    def name_to_qnode(cls, name: str) -> str:
        'Convert date interval name to corresponding qnode'
        name = name.lower()
        try:
            return cls.name_qnode_map[name]
        except KeyError:
            raise ValueError(f'Illegal data interval name: {name}')

    @classmethod
    def qnode_to_name(cls, qnode: str) -> str:
        'Convert date interval name to corresponding qnode'
        try:
            return cls.qnode_name_map[qnode]
        except KeyError:
            raise ValueError(f'Illegal data interval name: {qnode}')

    @classmethod
    def int_to_name(cls, precision: int) -> str:
        'Convert precision to corresponding date interval name'
        try:
            return cls.int_name_map[precision]
        except KeyError:
            raise ValueError(f'Illegal precision value: {precision}')

    @classmethod
    def name_to_int(cls, name: str) -> int:
        'Convert precision to corresponding date interval name'
        try:
            return cls.name_int_map[name]
        except KeyError:
            raise ValueError(f'Illegal data interval name: {name}')

    @classmethod
    def is_name(cls, name) -> bool:
        return name in cls.name_int_map


class TimePrecision:
    int_name_map = {
        0: 'billion years',
        1: 'hundred million years',
        3: 'million years',
        4: 'hundred thousand years',
        5: 'ten thousand years',
        6: 'millennium',
        7: 'century',
        8: 'decade',
        9: 'year',
        10: 'month',
        11: 'day',
        12: 'hour',
        13: 'minute',
        14: 'second'
    }

    name_int_map = {value: key for key, value in int_name_map.items()}

    @classmethod
    def to_name(cls, precision: int) -> str:
        'Convert time precision integer to corresponding name'
        try:
            return cls.int_name_map[precision]
        except KeyError:
            raise ValueError(f'Illegal precision: {precision}')

    @classmethod
    def to_int(cls, name: str) -> int:
        'Convert time precision integer to corresponding name'
        name = name.lower()
        try:
            return cls.name_int_map[name]
        except KeyError:
            raise ValueError(f'Illegal precision value: {name}')

    @classmethod
    def is_name(cls, name: str) -> int:
        return name in cls.name_int_map

class Literal:
    @staticmethod
    def time_int_precision(datetime: str, precision: int) -> str:
        # remove microsecond
        datetime = f'{datetime}'.split('.')[0]
        if precision:
            return f"^{datetime}/{precision}"
        else:
            return f"^{datetime}"

    @staticmethod
    def time_str_precision(datetime: str, precision_name: str) -> str:
        if precision_name:
            precision = TimePrecision.to_int(precision_name)
            return f"^{datetime}/{precision}"
        else:
            return f"^{datetime}"

def get_edges_from_request() -> pd.DataFrame:
    try:
        edges = pd.read_csv(request.stream, sep='\t', quoting=csv.QUOTE_NONE, dtype=object).fillna('')
    except:
        raise ValueError({ 'Error': 'Invalid input passed, expected TSV body' })

    # if request.files is None:
    #     content = {
    #         'Error': 'Missing TSV edge file'
    #     }
    #     raise ValueError(content)

    # for key, file_storage in request.files.items():
    #     edges = pd.read_csv(file_storage, sep='\t', quoting=csv.QUOTE_NONE, dtype=object).fillna('')
    #     # Get just the first file
    #     break

    valid_column_names = ['node1', 'label', 'node2']
    if not set(edges.columns) == set(valid_column_names):
        content = {
            'Error': f'Invalid TSV columns: {str(set(edges.columns))}. Expecting: {valid_column_names}'
        }
        raise ValueError(content)

    edges = edges.loc[:, valid_column_names]

    edges = kgtk_replacement.add_ids(edges)

    return edges
