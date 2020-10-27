import datetime
import gzip
import json
import typing
import os
import sys

from dateutil.parser import parse
from enum import Enum

from api.util import DataInterval, Literal, TimePrecision

from api.variable.put import CanonicalData
from db.sql import dal

pcd = CanonicalData()

DEFAULT_DATE = datetime.datetime(1900, 1, 1)

class DataType(Enum):
    QNODE = 1
    PNODE = 2
    QLIST = 3
    PLIST = 4
    STRING = 5
    URL = 6
    DATE = 7
    PRECISION = 8
    INTERVAL = 9
    INTEGER = 10
    FLOAT = 11

PROPERTY_LABEL = {
    'P17': 'country',
    'P31': 'instance of',
    'P170': 'creator',
    'P276': 'location',
    'P275': 'copyrightLicense',
    'P356': 'doi',
    'P527': 'hasPart',
    'P580': 'start time',
    'P582': 'end time',
    'P767': 'contributor',
    'P921': 'main subject',
    'P1114': 'quantity',
    'P1476': 'name',
    'P1687': 'Wikidata property',
    'P1813': 'short name',
    'P1880': 'measurement scale',
    'P1932': 'stated as',
    'P2699': 'url',
    'P2860': 'cites work',
    'P3896': 'geoshape',
    'P6269': 'apiEndpoint',
    'P6339': 'data interval',
    'label': '',
    'description': 'description',
    'schema:dateCreated': 'dateCreated',
    'schema:includedInDataCatalog': 'includedInDataCatalog',
    'schema:keywords': 'keywords',
    'schema:version': 'version',
    'P2006020004': 'dataset',
    'P2006020002': 'has qualifier',
    'P2006020003': 'variable measured',
    'P2006020005': 'mapping file'
}

def isQnode(name: str) -> bool:
    if not isinstance(name, str):
        return False
    try:
        if not name[0] == 'Q':
            return False
        int(name[1:])
        return True
    except:
        return False

def isPnode(name: str) -> bool:
    if not isinstance(name, str):
        return False
    try:
        if not name[0] == 'P':
            return False
        int(name[1:])
        return True
    except:
        return False

# TODO: Replace with actual wikifier
def wikify(description: str):
    if description.startswith('Q'):
        return description
    return 'Q'+description

# TODO: Replace with actual wikifier
def wikify_property(description: str):
    if description.startswith('P'):
        return description
    return 'P'+description

def process_qnode_obj(item: typing.Union[str, dict]) -> dict:
    if isinstance(item, str):
        if isQnode(item):
            qnode = item
            name = dal.get_label(qnode, qnode)
        else:
            name = item
            qnode = wikify(item)
    elif isinstance(item, dict):
        if not ('name' in item and 'identifier' in item):
            raise ValueError('Must have "name" and "identifier" keys in object.')
        name = item['name']
        qnode = item['identifier']
    else:
        raise ValueError(f'Not recognized qnode object type: {type(item)}')
    return {'name': name, 'identifier': qnode}

def process_pnode_obj(item: typing.Union[str, dict]) -> dict:
    if isinstance(item, str):
        if isQnode(item):
            qnode = item
            name = dal.get_label(qnode, qnode)
        else:
            name = item
            qnode = wikify_property(item)
    elif isinstance(item, dict):
        if not 'name' in item or not 'identifier' in item:
            raise ValueError('Must have "name" and "identifier" keys in object.')
        name = item['name']
        qnode = item['identifier']
    else:
        raise ValueError(f'Not recognized qnode object type: {type(item)}')
    return {'name': name, 'identifier': qnode}

def process_qnode_list(qlist: typing.List) -> typing.List[dict]:
    result = []
    for item in qlist:
        result.append(process_qnode_obj(item))
    return result

def process_pnode_list(qlist: typing.List) -> typing.List[dict]:
    result = []
    for item in qlist:
        result.append(process_pnode_obj(item))
    return result


class Metadata:

    # official properties
    _datamart_fields: typing.List[str] = []

    _datamart_field_type: typing.Dict[str, DataType] = {}

    # properties for internal use
    _internal_fields: typing.List[str] = []

    # minimal required properties
    _required_fields: typing.List[str] = []

    # properties for GET metadata collection methods
    _collection_get_fields: typing.List[str] = []

    # properties containing lists
    _list_fields: typing.List[str] = []

    # mapping for properties to pnodes
    _name_to_pnode_map: typing.Dict[str, str] = {}

    def __init__(self):
        self._unseen_properties = []
        for attr in self._datamart_fields:
            setattr(self, attr, None)
            # if attr in self._name_to_pnode_map:
            #     setattr(self, f'_{attr}_pnode', self._name_to_pnode_map[attr])
        for attr in self._internal_fields:
            setattr(self, attr, None)

    def __setattr__(self, name, value):
        if name.startswith('_') or name in self._datamart_fields or name in self._internal_fields:
            super().__setattr__(name, value)
        else:
            raise ValueError(f'attribute not allowed: {name}')

    @classmethod
    def fields(cls) -> list:
        return cls._datamart_fields

    @classmethod
    def is_required_field(cls, field: str) -> bool:
        return field in cls._required_fields

    @classmethod
    def is_list_field(cls, field: str) -> bool:
        return field in cls._list_fields

    def field_edge(self, node1: str, field_name: str, *, required: bool = False,
                   is_time: bool = False, is_item: bool = False):
        value = getattr(self, field_name, None)
        label = self._name_to_pnode_map[field_name]

        if value is None:
            if required:
                raise ValueError(f'Missing field for node {node1}: {field_name}')
            return
        if isinstance(value, dict):
            if 'identifier' in value:
                value = value['identifier']
            else:
                raise ValueError(f'Do not know how to handle dict: {node1} {field_name} {value}')
        elif not isinstance(value, (float, int, str)):
            raise ValueError(f'Do not know how to handle: {node1} {field_name} {value}')

        if is_time:
            precision = getattr(self, f'{field_name}_precision')
            edge = pcd.create_triple(
                # node1, label, json.dumps(Literal.time_int_precision(value, precision)))
                node1, label, Literal.time_int_precision(value, precision))
        elif is_item:
            if isinstance(value, str):
                if not (value.startswith('Q') or value.startswith('P')):
                    print(f'Object for {field_name} should be a qnode or pnode: {value}')
                edge = pcd.create_triple(node1, label, value)
            elif isinstance(value, dict):
                edge = pcd.create_triple(node1, label, value['identifier'])
            else:
                edge = pcd.create_triple(node1, label, value)
        else:
            edge = pcd.create_triple(node1, label, json.dumps(value))
        return edge

    def update(self, metadata: dict) -> None:
        for key, value in metadata.items():
            setattr(self, key, value)

    def to_dict(self, *, collection_get_fields=False, include_internal_fields=False) -> dict:
        result = {}
        if collection_get_fields:
            for attr in self._collection_get_fields:
                if getattr(self, attr):
                    result[attr] = getattr(self, attr)
            return result

        for attr in self._datamart_fields:
            if getattr(self, attr):
                result[attr] = getattr(self, attr)
        if include_internal_fields:
            for attr in self._internal_fields:
                if getattr(self, attr):
                    result[attr] = getattr(self, attr)
        return result

    def from_dict(self, desc: dict):
        for key, value in desc.items():
            if key in self._datamart_fields or key in self._internal_fields:
                setattr(self, key, value)
            else:
                raise ValueError(f'Key not allowed: {key}')
        return self

    def to_json(self, *, include_internal_fields=False, **kwargs) -> str:
        return json.dumps(self.to_dict(include_internal_fields=include_internal_fields), **kwargs)

    def from_json(self, desc: str):
        return self.from_dict(json.loads(desc))

    def from_request(self, desc: dict) -> typing.Tuple[dict, int]:
        '''Process description from REST request'''
        result = {}
        error = {}

        # Check required fields
        for required in self._required_fields:
            if required not in desc:
                error['Error'] = 'Missing required properties'
                if 'Missing' in error:
                    error['Missing'].append(required)
                else:
                    error['Missing'] = [required]
        if error:
            return error, 400

        # Parse each field
        for name in self._datamart_fields:
            value = desc.get(name, None)
            if not value:
                continue
            data_type = self._datamart_field_type[name]

            # print(name, data_type, value)

            if data_type == DataType.QNODE:
                try:
                    result[name] = process_qnode_obj(value)
                except ValueError as err:
                    error[name] = str(err)
            elif data_type == DataType.PNODE:
                try:
                    result[name] = process_pnode_obj(value)
                except ValueError as err:
                    error[name] = str(err)
            elif data_type == DataType.QLIST:
                if isinstance(value, list):
                    try:
                        result[name] = process_qnode_list(value)
                    except ValueError as err:
                        error[name] = str(err)
                else:
                    error[name] = 'Expecting a list'
            elif data_type == DataType.PLIST:
                if isinstance(value, list):
                    try:
                        result[name] = process_pnode_list(value)
                    except ValueError as err:
                        error[name] = str(err)
                else:
                    error[name] = 'Expecting a list'
            elif data_type == DataType.STRING:
                if isinstance(value, str):
                    result[name] = value
                else:
                    error[name] = 'Expecting a string'
            elif data_type == DataType.URL:
                if isinstance(value, str):
                    # TODO: validate URL
                    result[name] = value
                else:
                    error[name] = 'Expecting a string'
            elif data_type == DataType.DATE:
                try:
                    result[name] = parse(value, default=DEFAULT_DATE).isoformat()
                    # print(name, result[name])
                except (ValueError, OverflowError) as error:
                    print(error)
                    error[name] = str(error)
            elif data_type == DataType.PRECISION:
                if TimePrecision.is_name(value):
                    result[name] = TimePrecision.to_int(value)
                else:
                    error[name] = f'Precision value not recognized: {value}'
            elif data_type == DataType.INTERVAL:
                if DataInterval.is_name(value):
                    result[name] = DataInterval.name_to_qnode(value)
                else:
                    error[name] = f'Value not recognized: {value}'
            elif data_type == DataType.INTEGER:
                if isinstance(value, int):
                    result[name] = value
                else:
                    try:
                        result[name] = int(value)
                    except:
                        error[name] = f'Value not recognized int value: {value}'
            elif data_type == DataType.FLOAT:
                if isinstance(value, float):
                    result[name] = value
                else:
                    try:
                        result[name] = float(value)
                    except:
                        error[name] = f'Value not recognized int value: {value}'
            else:
                return {'Unknow datatype': data_type}, 500
        if error:
            error['Error'] = 'Cannot parse JSON body'
            return error, 400
        self.from_dict(result)
        return {}, 200


class DatasetMetadata(Metadata):
    '''
    Datamart dataset metadata.
    See: https://datamart-upload.readthedocs.io/en/latest/
    '''

    _datamart_fields = [
        'name',
        'description',
        'url',
#        'short_name',
        'dataset_id',
        'keywords',
        'creator',
        'contributor',
        'cites_work',
        'copyright_license',
        'version',
        'doi',
        'main_subject',
        'coordinate_location',
        'geoshape',
        'country',
        'location',
        'start_time',
        'end_time',
        'start_time_precision',
        'end_time_precision',
        'data_interval',
        'variable_measured',
        'mapping_file',
        'official_website',
        'date_created',
        'api_endpoint',
        'included_in_data_catalog',
        'has_part'
    ]
    _required_fields = [
        'name',
        'description',
        'url',
        'dataset_id'
    ]
    _collection_get_fields = [
        'name',
        'description',
        'url',
        'dataset_id'
#        'short_name'
    ]
    _internal_fields = [
        '_dataset_id'  # qnode
    ]
    _datamart_field_type = {
        'name': DataType.STRING,
        'description': DataType.STRING,
        'url': DataType.URL,
#         'short_name': DataType.STRING,
        'dataset_id': DataType.STRING,
        'keywords': DataType.STRING,
        'creator': DataType.QNODE,
        'contributor': DataType.QNODE,
        'citesWork': DataType.STRING,
        'copyright_license': DataType.QNODE,
        'version': DataType.STRING,
        'doi': DataType.STRING,
        'main_subject': DataType.QNODE,
        'coordinate_location': DataType.STRING,
        'geoshape': DataType.STRING,
        'country': DataType.QNODE,
        'location': DataType.QNODE,
        'start_time': DataType.DATE,
        'end_time': DataType.DATE,
        'startTime_precision': DataType.PRECISION,
        'endTime_precision': DataType.PRECISION,
        'data_interval': DataType.INTERVAL,
        'variable_measured': DataType.QNODE,
        'mapping_file': DataType.URL,
        'official_website': DataType.URL,
        'date_created': DataType.DATE,
        'api_endpoint': DataType.URL,
        'included_in_data_catalog': DataType.QNODE,
        'has_part': DataType.URL
    }
    _name_to_pnode_map = {
        'name': 'P1476',
        'description': 'description',
        'url': 'P2699',
        # 'short_name': 'P1813',
        # 'datasetID': 'None',
        'dataset_id': 'P1813',
        'keywords': 'schema:keywords',
        'creator': 'P170',
        'contributor': 'P767',
        'cites_work': 'P2860',
        'copyright_license': 'P275',
        'version': 'schema:version',
        'doi': 'P356',
        'main_subject': 'P921',
        'coordinate_location': 'P921',
        'geoshape': 'P3896',
        'country': 'P17',
        'location': 'P276',
        'start_time': 'P580',
        'end_time': 'P582',
        'data_interval': 'P6339',
        'variable_measured': 'P2006020003',
        'mapping_file': 'P2006020005',
        'official_website': 'P856',
        'date_created': 'schema:dateCreated',
        'api_endpoint': 'P6269',
        'included_in_data_catalog': 'schema:includedInDataCatalog',
        'has_part': 'P527'

    }
    def __init__(self):
        super().__init__()
        self.name = None
        self.description = None
        self.url = None
        # self.short_name = None
        self.dataset_id = None
        self.keywords = None
        self.creator = None
        self.contributor = None
        self.cites_work = None
        self.copyright_license = None
        self.version = None
        self.doi = None
        self.main_subject = None
        self.coordinate_location = None
        self.geoshape = None
        self.country = None
        self.location = None
        self.start_time = None
        self.end_time = None
        self.data_interval = None
        self.variable_measured = None
        self.mapping_file = None

    def to_kgtk_edges(self, dataset_node) -> typing.List[dict]:

        edges = []

        # isa data set
        edge = pcd.create_triple(dataset_node, 'P31', 'Q1172284')
        edges.append(edge)

        # stated as
        # edges.append(create_triple(edge['id'], 'P1932', json.dumps(self.shortName)))

        # label and title
        edges.append(pcd.create_triple(dataset_node, 'label', json.dumps(self.name)))
        edges.append(self.field_edge(dataset_node, 'name', required=True))
        edges.append(self.field_edge(dataset_node, 'description', required=True))
        edges.append(self.field_edge(dataset_node, 'url', required=True))
        edges.append(self.field_edge(dataset_node, 'dataset_id', required=True))

        # Optional
        # edges.append(self.field_edge(dataset_node, 'short_name'))
        edges.append(self.field_edge(dataset_node, 'keywords'))
        edges.append(self.field_edge(dataset_node, 'creator'))
        edges.append(self.field_edge(dataset_node, 'contributor'))
        edges.append(self.field_edge(dataset_node, 'cites_work'))
        edges.append(self.field_edge(dataset_node, 'copyright_license', is_item=True))
        edges.append(self.field_edge(dataset_node, 'version'))
        edges.append(self.field_edge(dataset_node, 'doi'))
        edges.append(self.field_edge(dataset_node, 'main_subject', is_item=True))
        edges.append(self.field_edge(dataset_node, 'geoshape'))
        edges.append(self.field_edge(dataset_node, 'country', is_item=True))
        edges.append(self.field_edge(dataset_node, 'location', is_item=True))
        edges.append(self.field_edge(dataset_node, 'start_time', is_time=True))
        edges.append(self.field_edge(dataset_node, 'end_time', is_time=True))
        edges.append(self.field_edge(dataset_node, 'data_interval', is_item=True))
        edges.append(self.field_edge(dataset_node, 'variable_measured', is_item=True))
        edges.append(self.field_edge(dataset_node, 'mapping_file'))

        edges = [edge for edge in edges if edge is not None]
        return edges


class VariableMetadata(Metadata):
    '''
    Datamart variable metadata.
    See: https://datamart-upload.readthedocs.io/en/latest/
    '''
    _datamart_fields = [
        'name',
        'variable_id',
        'dataset_id',
        # 'short_name',
        # 'dataset_short_name',
        'description',
        'corresponds_to_property',
        'main_subject',
        'unit_of_measure',
        'country',
        'location',
        'start_time',
        'end_time',
        'start_time_precision',
        'end_time_precision',
        'data_interval',
        'column_index',
        'qualifier',
        'count',
        'tag'
    ]
    _required_fields = [
        'name',
    ]
    _collection_get_fields = [
        'name',
        'variable_id',
        'dataset_id'
    ]

    _internal_fields = [
        '_dataset_id',  # qnode
        '_variable_id',  # qnode
        '_property_id',  # pnode
        '_aliases',
        '_max_admin_level',
        '_precision'
    ]
    _list_fields = ['main_subject', 'unit_of_measure', 'country', 'qualifier']
    _datamart_field_type = {
        'name': DataType.STRING,
        'variable_id': DataType.STRING,
        'dataset_id': DataType.STRING,
        # 'short_name': DataType.STRING,
        # 'dataset_short_name': DataType.STRING,
        'description': DataType.STRING,
        'corresponds_to_property' : DataType.PNODE,
        'main_subject': DataType.QLIST,
        'unit_of_measure': DataType.QLIST,
        'country': DataType.QLIST,
        'location': DataType.QLIST,
        'start_time': DataType.DATE,
        'end_time': DataType.DATE,
        'start_time_precision': DataType.PRECISION,
        'end_time_precision': DataType.PRECISION,
        'data_interval': DataType.INTERVAL,
        'column_index': DataType.STRING,
        'qualifier': DataType.QLIST,
        'count': DataType.INTEGER
    }
    _name_to_pnode_map = {
        'name': 'P1476',
        # 'variableID': 'None',
        # 'datasetID': 'None',
        # 'short_name': 'P1813',
        'variable_id': 'P1813',
        'description': 'description',
        'corresponds_to_property': 'P1687',
        'main_subject': 'P921',
        'unit_of_measure': 'P1880',
        'country': 'P17',
        'location': 'P276',
        'start_time': 'P580',
        'end_time': 'P582',
        'data_interval': 'P6339',
        'column_index': 'P2006020001',
        'qualifier': 'P2006020002',
        'count': 'P1114'
    }

    def __init__(self):
        super().__init__()
        self.name = None
        self.variable_id = None
        self.dataset_id = None
        # self.short_name = None
        self.description = None
        self.corresponds_to_property = None
        self.main_subject = []
        self.unit_of_measure = []
        self.country = []
        self.location = []
        self.start_time = None
        self.end_time = None
        self.start_time_precision = None
        self.end_time_precision = None
        self.data_interval = None
        self.column_index: typing.Union(int, None) = None
        self.qualifier = []
        self.count = None

        self._max_admin_level = None
        self._precision = None

    def to_kgtk_edges(self, dataset_node: str, variable_node, defined_labels: set = None,
    ) -> typing.List[dict]:

        edges = []

        # is instance of variable
        edge = pcd.create_triple(variable_node, 'P31', 'Q50701')
        edges.append(edge)

        # edges.append(create_triple(edge['id'], 'P1932', self.variableID))

        # has title
        edges.append(pcd.create_triple(variable_node, 'label', json.dumps(self.name)))
        edges.append(self.field_edge(variable_node, 'name', required=True))

        # edges.append(self.field_edge(variable_node, 'short_name', required=True))
        edges.append(self.field_edge(variable_node, 'variable_id', required=True))

        edges.append(self.field_edge(variable_node, 'description'))

        edges.append(pcd.create_triple(dataset_node, 'P2006020003', variable_node))
        edges.append(pcd.create_triple(variable_node, 'P2006020004', dataset_node))

        # Wikidata property (P1687) expects object to be a property. KGTK
        # does not support object with type property (May 2020).
        # edges.append(pcd.create_triple(variable_node, 'P1687', self.corresponds_to_property))
        edges.append(self.field_edge(variable_node, 'corresponds_to_property', is_item=True))

        if self.unit_of_measure:
            for unit in self.unit_of_measure:
                edges.append(pcd.create_triple(variable_node, 'P1880', unit['identifier']))
                if defined_labels is not None and unit['identifier'] not in defined_labels:
                    defined_labels.add(unit['identifier'])
                    edges.append(pcd.create_triple(unit['identifier'], 'label', json.dumps(unit['name'])))

        if self.main_subject:
            for main_subject_obj in self.main_subject:
                edges.append(
                    pcd.create_triple(variable_node, 'P921', main_subject_obj['identifier']))

        # precision = DataInterval.name_to_int(self.data_interval)
        edges.append(self.field_edge(variable_node, 'start_time', is_time=True))
        edges.append(self.field_edge(variable_node, 'end_time', is_time=True))

        if self.data_interval:
            edges.append(pcd.create_triple(
                # variable_node, 'P6339', DataInterval.name_to_qnode(self.data_interval))
                variable_node, 'P6339', self.data_interval))

        edges.append(self.field_edge(variable_node, 'column_index'))

        edges.append(self.field_edge(variable_node, 'count'))

        if self.qualifier:
            for qualifier_obj in self.qualifier:
                qualifier_node = qualifier_obj['identifier']
                if qualifier_node.startswith('pq:'):
                    qualifier_node = qualifier_node[3:]
                edge = pcd.create_triple(variable_node, 'P2006020002', qualifier_node)
                edges.append(edge)
                # qualifier stated as
                edges.append(pcd.create_triple(edge['id'], 'P1932', json.dumps(qualifier_obj['name'])))

        if self.country:
            for country_obj in self.country:
                edges.append(pcd.create_triple(variable_node, 'P17', country_obj['identifier']))

        if self.location:
            for location_obj in self.location:
                edges.append(pcd.create_triple(variable_node, 'P276', location_obj['identifier']))

        edges = [edge for edge in edges if edge is not None]
        return edges
