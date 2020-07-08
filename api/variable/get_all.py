from flask import request, make_response, current_app

import pandas as pd

from db.sql import dal
from db.sql.dal import Region
from api.region_cache import region_cache
from api.variable.get import UnknownSubjectError
from api.variable.get import VariableGetter
from api.metadata.main import VariableMetadataResource


class VariableGetterAll:
    vg = VariableGetter()
    vmr = VariableMetadataResource()

    def get(self, dataset):

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
        variables = [x['variable_id'] for x in self.vmr.get(dataset)[0]][:limit]
        print(variables)

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
