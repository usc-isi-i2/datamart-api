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

        if len(request_variables) > 0:
            variables = request_variables
        else:
            variables = [x['variable_id'] for x in self.vmr.get(dataset)[0]][:limit]

        df_list = []
        for variable in variables:
            df_list.append(
                self.vg.get_direct(dataset, variable, include_cols, exclude_cols, -1, regions, return_df=True))
        df = pd.concat(df_list)

        csv = df.to_csv(index=False)
        output = make_response(csv)
        output.headers['Content-Disposition'] = f'attachment; filename={dataset}_variables_all.csv'
        output.headers['Content-type'] = 'text/csv'
        return output
