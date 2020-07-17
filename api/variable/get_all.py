import pandas as pd
from flask import request, make_response
from api.variable.get import VariableGetter
from api.variable.get import UnknownSubjectError
from api.metadata.main import VariableMetadataResource


class VariableGetterAll:
    vg = VariableGetter()
    vmr = VariableMetadataResource()

    def get(self, dataset):
        request_variables = request.args.getlist('variable') or []
        include_cols = request.args.getlist('include') or []
        exclude_cols = request.args.getlist('exclude') or []

        limit = 20
        if request.args.get('limit') is not None:
            try:
                limit = int(request.args.get('limit'))
            except:
                pass

        try:
            regions = self.vg.get_query_region_ids()
        except UnknownSubjectError as ex:
            return ex.get_error_dict(), 404

        variables_metadata = []
        if len(request_variables) > 0:
            variables = request_variables
            for v in variables:
                variables_metadata.append(self.vmr.get(dataset, variable=v)[0])
        else:
            variables_metadata = self.vmr.get(dataset)[0]

        variables_metadata = variables_metadata[:limit]
        df_list = []
        for variable in variables_metadata:
            _ = self.vg.get_direct(dataset, variable['variable_id'], include_cols, exclude_cols, -1, regions,
                                   return_df=True)
            qualifiers = variable['qualifier']
            generic_qualifiers = [x['name'] for x in qualifiers if x['identify'] not in ('P585', 'P248')]

            if _ is not None:
                _ = self.reshape_canonical_data(_, generic_qualifiers)
                df_list.append(_)
        df = pd.concat(df_list)
        # df.drop(columns=['value', 'value_unit', 'variable_id', 'variable'], inplace=True)
        csv = df.to_csv(index=False)
        output = make_response(csv)
        output.headers['Content-Disposition'] = f'attachment; filename={dataset}_variables_all.csv'
        output.headers['Content-type'] = 'text/csv'
        return output

    def reshape_canonical_data(self, df, qualifier_columns_to_reshape):
        new_df = pd.DataFrame(columns=df.columns)
        for i, row in df.iterrows():
            row[f"{row['variable_id']}"] = row['value']
            row['{}_UNIT'.format(row['variable_id'])] = row['value_unit']
            row['{}_NAME'.format(row['variable_id'])] = row['variable']
            for q in qualifier_columns_to_reshape:
                row['{}_QUALIFIER_{}'.format(row['variable_id'], q)] = row[q]

            new_df = new_df.append(row)
        c_to_d = qualifier_columns_to_reshape
        c_to_d.extend(['value', 'value_unit', 'variable_id', 'variable'])
        new_df.drop(columns=c_to_d, inplace=True)
        return new_df
