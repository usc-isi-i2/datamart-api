import pandas as pd
from pandas import DataFrame, Series

def get_index(series: Series, value, *, pos=0) -> int:
    ''' Get the index of a key in a pandas.Series
    '''
    return int(series[series == value].index[pos])

def locate_data_offset(df_template: DataFrame) -> int:
    ''' Locate the 'data' tag offset relative to the 'header' tag
        If 'data' tag is not present in the template,
            we assume 'data' tag is one row below 'header' tag,
        Args:
            df_template: template DataFrame
        Returns:
            offset: the row difference between 'data' and 'header' tag minus one
        Note:
            If the two numbers are present at $F1 and $G1 of the template file,
                the program will use these two numbers directly by default
    '''
    # Detect if header row and data row has already been recorded
    try:
        header_index = int(df_template.iat[0, 5])
        data_index = int(df_template.iat[0, 6])
    except:
        header_index = get_index(df_template.iloc[:, 0], 'header')
        try:
            data_index = get_index(df_template.iloc[:, 0], 'data')
        except IndexError: # No data tag found, assume the first row below header tag
            data_index = header_index + 1
        except:
            raise ValueError('Error: Unknown Error when locating data offset')
    finally:
        # datatag starts at the 0th row of the spreadsheet
        return data_index - header_index - 1

def find_data_start_row(df: DataFrame) -> (int, int):
    ''' Find the header row number and data row number
        Args:
            df_template: template DataFrame
        Returns:
            (header_index, data_index): 0-based indices
    '''
    # finds and returns header and data row index
    header_index = get_index(df.iloc[:, 0], 'header')
    data_index = get_index(df.iloc[:, 0], 'data')
    return header_index, data_index

def save_annotation_template(df: DataFrame, template_path: str) -> bool:
    ''' Save the template annotation
        Args:
            df_template: template DataFrame
            template_path: path where the file will be stored
        Returns:
            bool: whether the operation succeeded
        Note:
            1. Header index and data index would be stored at $F1 and $G1 of the template
            2. The file stored assumes to be a *.tsv file
    '''
    if not template_path.endswith('.tsv'):
        print('Error: Must save template as a .tsv file!')
        return False

    try:
        header_row, data_row = find_data_start_row(df)
    except:
        return False
    annotation_rows = list(range(0, 7)) + [header_row]
    annotation = df.iloc[annotation_rows].fillna("")
    annotation.iat[0, 5] = header_row
    annotation.iat[0, 6] = data_row
    annotation.to_csv(template_path, index=False, header=None, sep='\t')
    return True
