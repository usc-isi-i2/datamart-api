
# A provider for performing the necessary SPARQL queries.
# This was taken from the interim wikidata fuzzysearch backend, and is not fully working at all, which is
# why this file is commented out

# class SPARQLProvider:    
#     def query_variable(self, dataset, variable):
#         query = f'''
# select ?dataset_id ?variable_id ?variable_name ?property_id
# where {{
#   ?dataset_ wdt:P1813 ?dname .
#   FILTER (str(?dname) = "{dataset}")
#   ?variable_ wdt:P361 ?d .
#   ?variable_ wdt:P1813 ?vname .
#   ?variable_ rdfs:label ?variable_name .
#   FILTER (str(?vname) = "{variable}")
#   ?variable_ wdt:P1687 ?property_ .
#   BIND(REPLACE(STR(?dataset_), "(^.*)(Q.+$)", "$2") AS ?dataset_id)
#   BIND(REPLACE(STR(?variable_), "(^.*)(Q.+$)", "$2") AS ?variable_id)
#   BIND(REPLACE(STR(?property_), "(^.*)(Q.+$)", "$2") AS ?property_id)
# }}
# '''
#         print(query)
#         sparql.setQuery(query)
#         sparql.setReturnFormat(JSON)
#         result = sparql.query()
#         response = result.convert()
#         print(response)
#         if response['results']['bindings']:
#             binding = response['results']['bindings'][0]
#             return {
#                 'dataset_id': binding['dataset_id']['value'],
#                 'variable_id': binding['variable_id']['value'],
#                 'property_id': binding['property_id']['value'],
#                 'variable_name': binding['variable_name']['value']
#             }
#         return {}

#     def query_qualifiers(self, variable_id, property_id):
#         query = f'''
# select ?qualifier_id ?qual_name
# where {{
#   wd:{variable_id} p:{property_id} ?st .
#   ?st ps:{property_id} ?qual_ .
#   ?st pq:P1932 ?qual_name .
#   BIND(REPLACE(STR(?qual_), "(^.*)(P.+$)", "$2") AS ?qualifier_id)
# }}
# '''
#         print(query)
#         sparql.setQuery(query)
#         sparql.setReturnFormat(JSON)
#         result = sparql.query()
#         response = result.convert()
#         print(response)
#         qualifiers = {binding['qualifier_id']['value']:binding['qual_name']['value']
#                       for binding in response['results']['bindings']}
#         return qualifiers

#     def query_data(self, dataset_id, property_id, places, qualifiers, limit, cols):
#         # Places are not implemented in SPARQL yet
#         select_columns = '?dataset ?main_subject_id ?value ?value_unit ?time ?coordinate ' + ' '.join(f'?{name}_id' for name in qualifiers.values())

#         qualifier_query = ''
#         for pq_property, name  in qualifiers.items():
#             qualifier_query += f'''
#   ?o {pq_property} ?{name}_ .
#   BIND(REPLACE(STR(?{name}_), "(^.*)(Q.\\\\d+$)", "$2") AS ?{name}_id)
# '''
#         dataset_query = self._get_direct_dataset_query(
#             property_id, select_columns, qualifier_query, limit)
#         print(dataset_query)

#         sparql.setQuery(dataset_query)
#         sparql.setReturnFormat(JSON)
#         result = sparql.query()
#         response = result.convert()

#         parsed = self._parse_response(response, dataset_id, cols)
#         return parsed

#     def _get_direct_dataset_query(self, property_id, select_columns, qualifier_query, limit):

#         dataset_query = f'''
# SELECT {select_columns} WHERE {{
#   VALUES(?property_id_ ?p ?ps ?psv) {{
#       (wd:{property_id} p:{property_id} ps:{property_id} psv:{property_id})
#   }}

#   ?main_subject_ ?p ?o .

#   # ?o ?ps ?value .
#   ?o ?psv ?value_obj .
#   ?value_obj wikibase:quantityAmount ?value .
#   optional {{
#     ?value_obj wikibase:quantityUnit ?unit_id .
#     ?unit_id rdfs:label ?value_unit .
#     FILTER(LANG(?value_unit) = "en")
#   }}

#   ?o pq:P585 ?time .

#   optional {{
#     ?main_subject_ wdt:P625 ?coordinate
#   }}

#   optional {{
#     ?o pq:P2006020004 ?dataset_ .
#     BIND(REPLACE(STR(?dataset_), "(^.*/)(Q.*)", "$2") as ?dataset)
#   }}

#   {qualifier_query}

#   BIND(REPLACE(STR(?main_subject_), "(^.*/)(Q.*)", "$2") AS ?main_subject_id)

# }}
# ORDER BY ?main_subject_id ?time
# '''
#         if limit > -1:
#             dataset_query = dataset_query + f'\nLIMIT {limit}'
#         print(dataset_query)
#         return dataset_query

#     def _parse_response(self, response, dataset_id, cols):
#         results = []
#         # for row, record in enumerate(response['results']['bindings']):
#         for record in response['results']['bindings']:
#             record_dataset = ''
#             if 'dataset' in record:
#                 record_dataset = record['dataset']['value']

#             # Skip record if dataset does not match
#             if record_dataset != dataset_id:
#                 # Make an exception for Wikidata, which does not have a dataset field
#                 if dataset_id == 'Wikidata' and record_dataset == '':
#                     pass
#                 else:
#                     print(f'Skipping: not {record_dataset} == Q{dataset_id}')
#                     # continue

#             result = {}
#             for col_name, typed_value in record.items():
#                 value = typed_value['value']
#                 if col_name in cols:
#                     result[col_name] = value
#                     # col = result_df.columns.get_loc(col_name)
#                     # result_df.iloc[row, col] = value
#                 if col_name not in COMMON_COLUMN.keys():

#                     # remove suffix '_id'
#                     qualifier = col_name[:-3]
#                     if qualifier not in cols:
#                         continue
#                     result[qualifier] = labels.get(value, value)
#                     # if value in metadata['qualifierLabels']:
#                     #     result[qualifier] = metadata['qualifierLabels'][value]
#                     #     # result_df.iloc[row, result_df.columns.get_loc(qualifier)] = metadata['qualifierLabels'][value]
#                     # else:
#                     #     print('missing qualifier label: ', value)
#                     #     result[qualifier] = value
#             results.append(result)
#         return results
