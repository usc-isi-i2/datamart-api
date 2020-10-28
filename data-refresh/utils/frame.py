import io, os
import pandas as pd
from utils.ops import upload_frame_annotated

def get_template(template_path: str) -> pd.core.frame.DataFrame:
    if not os.path.isfile(template_path):
        raise FileNotFoundError("Template file does not exist!")
    return pd.read_csv(template_path, sep='\t', dtype=object, header=None).fillna('')

def locate_data_offset(df_template: pd.core.frame.DataFrame) -> int:
    i = 0
    while i < len(df_template) and df_template.iloc[i,0] != 'data':
       i += 1
    return i - 7

def get_sheet(filename: str, nCols: int) -> pd.core.frame.DataFrame:

    # Only extract the first nCols columns specified in the template
    # Read with headers
    sheet = pd.DataFrame()
    if filename.endswith('.xlsx'):
        sheet = pd.read_excel(filename, dtype=object).fillna('')
    elif filename.endswith('.csv'):
        sheet = pd.read_csv(filename, encoding='latin1', dtype=object).fillna('')
    else:
        raise ValueError(f'Not supported type: {filename}')

    sheet = sheet[sheet.columns[:nCols]]
    return sheet

def append_sheet(annotated_sheet: pd.core.frame.DataFrame, filename: str, add_datatag: bool) -> pd.core.frame.DataFrame:

    # Nuber of variables to be uploaded, omit the first column (metadata)
    nCols = len(annotated_sheet.iloc[0]) - 1

    # Generate the sheet
    sheet = get_sheet(filename, nCols)

    # Verify Label matches
    if False in sheet.columns == df_template.iloc[6][1:]:
        raise ValueError(f'Columns do not match between template and input: {data_path}. Abort...')

    # Build inputs
    sheet.insert(loc=0, column='', value='')
    data_offset = locate_data_offset(annotated_sheet)

    if add_datatag:
        sheet.iloc[data_offset,0] = 'data'
        add_datatag = False

    # Build annotated data
    sheet.columns = annotated_sheet.columns
    annotated_sheet = annotated_sheet.iloc[:-1,:].append(sheet[data_offset:])

    return annotated_sheet, add_datatag

def upload_annotated_sheet(annotated_sheet: pd.core.frame.DataFrame, dataset_id: str, datamart_api_url: str) -> None:

    buffer = io.StringIO()
    annotated_sheet.to_csv(buffer, index=False, header=False)
    url = f'{datamart_api_url}/datasets/{dataset_id}/annotated?create_if_not_exist=true'
    upload_frame_annotated(buffer, url, False)
