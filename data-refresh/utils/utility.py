import io, os
import pandas as pd
from pandas import DataFrame

def generate_targets(datasets_path: str) -> [str]:
    ''' If the input dataset path is not a file, return the fuzzy string
            [glob] needs to find all relevant data files, otherwise
            just return itself as list
        Args:
            datasets_path: The path where data is located
        Returns:
            list(str): If input points to a file, it should return a list of size 1,
                        otherwise, it should return a list of size 2
    '''
    patterns = []
    if os.path.isfile(datasets_path):
        patterns.append(datasets_path)
    else:
        if datasets_path.endswith('/'):
            datasets_path = datasets_path[:-1]
        patterns.append(datasets_path + '/*.csv')
        patterns.append(datasets_path + '/*.xlsx')
    return patterns

def read_tsv(file_path: str) -> DataFrame:
    ''' Read a .tsv file as DataFrame from the path given
        Args:
            file_path: The path of the file
        Returns:
            DataFrame: The DataFrame of the file
    '''
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f'File {file_path} does not exist!')

    return pd.read_csv(file_path, sep='\t', dtype=object, header=None).fillna('')
