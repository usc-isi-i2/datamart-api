import io
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
from requests import get

def visualize_FSI(datamart_api_url: str, dataset_id: str = 'FSI',
                    q_variable: str = 'c1_security_apparatus', q_country: str = 'Gabon'):
    ''' Use plots to examine if the dataset from 'FSI' has been uploaded correctly
    '''
    response = get(f'{datamart_api_url}/datasets/{dataset_id}/variables/{q_variable}?country={q_country}')
    df = pd.read_csv(io.StringIO(response.text))

    y = df['value']
    x = df['time'].apply(lambda t: datetime.strptime(t,'%Y-%m-%dT%H:%M:%SZ').year)

    fig, ax = plt.subplots()
    ax.plot(x, y)

    ax.set(xlabel='year', ylabel=q_variable, title=f'Trend of {q_variable} of {q_country}')
    plt.show()

def visualize_WFP(datamart_api_url: str, dataset_id: str = 'WFP',
                    q_variable: str = 'price'):
    ''' Use plots to examine if the dataset from 'WFP' has been uploaded correctly
    '''
    response = get(f"{datamart_api_url}/datasets/{dataset_id}/variables/{q_variable}")
    df = pd.read_csv(io.StringIO(response.text))

    # Try to catch the trends of price of Sorghum (Retail)
    z = df[df['cmid']==65]
    z.reset_index(drop=True, inplace=True)
    # Generate y and x values
    z = pd.concat([z['value'], z['time'].apply(lambda t: datetime.strptime(t,'%Y-%m-%dT%H:%M:%SZ').year)], axis=1)
    z.columns = ['value', 'year']
    z = z[['value','year']].groupby(['year']).mean()

    fig, ax = plt.subplots()
    ax.plot(z.index,z['value'])

    ax.set(xlabel='year', ylabel='price (KG/ETB)', title=f'Trend of average price of Sorghum (Retail) in Ethiopia')
    plt.show()

def visualize_FAOSTAT(datamart_api_url: str, dataset_id: str = 'FAOSTAT',
                        q_variable: str = 'value'):
    ''' Use plots to examine if the dataset from 'FAOSTAT' has been uploaded correctly
    '''
    response = get(f'{datamart_api_url}/datasets/{dataset_id}/variables/{q_variable}')
    df = pd.read_csv(io.StringIO(response.text))

    # Select 'PK compounds'
    z = df[df['Item Code'] == 4027]
    # Select 'Production'
    z = z[z['Element Code'] == 5510]

    z = pd.concat([z['value'], z['time'].apply(lambda t: datetime.strptime(t,'%Y-%m-%dT%H:%M:%SZ').year)], axis=1)
    z.columns = ['value', 'year']
    z = z[['value','year']].groupby(['year']).sum()

    fig, ax = plt.subplots()
    ax.plot(z.index,z['value'])

    ax.set(xlabel='year', ylabel='tons', title=f'Trend of Total Production of PK Compounds Worldwide')
    plt.show()

def visualize(datamart_api_url: str, dataset_id: str):
    ''' Use plots to examine if the dataset has been uploaded correctly
        Currently supports 'FSI', 'WFP', and 'FAOSTAT'
    '''
    if dataset_id == 'FSI':
        visualize_FSI(datamart_api_url)
    elif dataset_id == 'WFP':
        visualize_WFP(datamart_api_url)
    elif dataset_id == 'FAOSTAT':
        visualize_FAOSTAT(datamart_api_url)
