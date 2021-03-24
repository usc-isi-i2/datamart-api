from datetime import datetime
from utils.get_ import variable_data
from typing import Optional, Tuple, NoReturn
import pandas as pd
from pandas import DataFrame
import matplotlib.pyplot as plt

def trend_df(df: DataFrame, variable_id: str):

    z = pd.concat([df['main_subject'], df['value'], df['time'].apply(lambda t: datetime.strptime(t,'%Y-%m-%dT%H:%M:%SZ').year)], axis=1)
    z.columns = ['main_subject', 'value', 'year']
    z = z[['value','year']].groupby(['year']).mean()

    fig, ax = plt.subplots()
    ax.plot(z.index,z['value'])

    ax.set(xlabel='year', ylabel='value', title=f'Trend of average {variable_id} throughout time')
    plt.show()

def trend(datamart_api_url: str, dataset_id: str, variable_id: str,
                auth: Optional[Tuple]=None) -> NoReturn:

    try:
        df = variable_data(datamart_api_url, dataset_id, variable_id, auth)
        trend_df(df, variable_id)
    except Exception as e:
        print('Error:', e)
