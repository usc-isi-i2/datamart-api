import glob
import pandas as pd
from pandas import DataFrame
from typing import Iterator
from utils.template import locate_data_offset
from utils.utility import generate_targets, read_tsv

def get_sheet(filename: str, nCols: int) -> DataFrame:
    ''' Get the data as spreadsheet/DataFrame from the filename specified
        Currently only supports '.xlsx' and '.csv' file
        Args:
            filename: The path of file where data is stored
            nCols: The number of columns to extract
                    (Sometimes not all columns are needed,
                        if they are present in template)
        Returns:
            sheet: A DataFrame containing the data
    '''

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

def append_sheet(annotated_sheet: DataFrame, filename: str, add_datatag: bool) -> DataFrame:
    ''' Append the data stored in [filename] to the end of annotated_sheet
        Args:
            annotated_sheet: May be a template (with no data), or annotated sheet (with data)
            filename: The path of file where data is stored
            add_datatag: A boolean value indicates if annotated_sheet comes with no data,
                            if yes, add 'data' tag as position indicated by [data_offset]
        Returns:
            annotated_sheet: A sheet including the old and new data
    '''

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
    annotated_sheet = annotated_sheet.iloc[:-1,:].append(sheet[data_offset:], ignore_index=True)

    return annotated_sheet, add_datatag

def create_annotated_sheet(template_path: str, dataset_path: str,
                            flag_combine_sheets: bool=False) -> Iterator[DataFrame]:
    '''Returns the iterator pointing to the new DataFrame created
        Args:
            template_path: filename of the template
            dataset_path: pathname of the dataset (could be a directory or file)
            flag_combine_sheets: Whether to combine sheets in different files
                                    or return them separatedly
        Returns:
            Iterator of the annotated sheet
    '''

    df_template = read_tsv(template_path)
    paths = generate_targets(dataset_path)

    add_datatag = True

    if flag_combine_sheets:
        # Combine the data files into one annotated sheet, and POST it to datamart (only once)
        annotated_sheet = df_template
        for p in paths:
            for filename in glob.iglob(p):
                annotated_sheet, add_datatag = append_sheet(annotated_sheet, filename, add_datatag)
        yield annotated_sheet
    else:
        # Annotate the files separatedly, and POST it to datamart (multiple times)
        for p in paths:
            for filename in glob.iglob(p):
                annotated_sheet, _ = append_sheet(df_template, filename, add_datatag)
                yield annotated_sheet
