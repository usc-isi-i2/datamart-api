from api.SQLProvider import SQLProvider

class VariableDeleter():
    def delete(self, dataset, variable):
        provider = SQLProvider()

        result = provider.query_variable(dataset, variable)
        if not result:
            content = {
                'Error': f'Could not find dataset {dataset} variable {variable}'
            }
            return content, 404

        provider.delete_variable(result['dataset_id'], result['variable_id'], result['property_id'])
        return {}, 204

