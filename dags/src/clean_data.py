import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

def clean():

    query = '''
        select * from apt_details_raw
        where date > '2025-01-01' and date < '2025-12-01'
        '''

    username = 'piotr'
    password = os.environ['MYSQL_PASSWORD']
    host = '192.168.0.133'
    db_name = 'airflow_db'
    db_url = f'mysql+mysqlconnector://{username}:{password}@{host}/{db_name}'

    engine = create_engine(db_url)

    with engine.connect() as conn:
        data_raw = pd.read_sql_query(sql=query, con=conn, index_col='id')

    print(f'PIOTR: raw data shape: {data_raw.shape}')

    # renaming market type
    data_raw.market.replace(['pierwotny','wtorny'], ['primary_market','aftermarket'], inplace=True)

    # removing foreign locations
    data_raw.drop(index=data_raw[data_raw.voivodeship=='zagranica'].index, axis=0, inplace=True)

    # removing data with no price and changig data type
    data_raw.drop(index=data_raw[data_raw.price=='Zapytajoce'].index, axis=0, inplace=True)

    # removing offers for rent
    data_raw.drop(index=data_raw[data_raw.offer_type=='wynajem'].index, axis=0, inplace=True)

    # remove records with missing data
    data_raw.dropna(subset=['price','localization_y','localization_x','area'], axis=0, inplace=True)
    data_raw.dropna(subset=['market','offer_type','rooms','floor'], axis=0, inplace=True)
    data_raw.dropna(subset=['build_yr'], axis=0, inplace=True)
    data_raw.dropna(subset=['floors'], axis=0, inplace=True)

    # cleaning categorical data
    data_raw.floor.replace('parter', '0', inplace=True)
    data_raw = data_raw[data_raw.floor != 'suterena']
    data_raw = data_raw[data_raw.floors != 'windą']

    # changing data types
    data_raw.localization_x = data_raw.localization_x.astype('float')
    data_raw.localization_y = data_raw.localization_y.astype('float')
    data_raw.area = data_raw.area.astype('float')
    data_raw.price = data_raw.price.astype('float')

    data_raw.rooms = data_raw.rooms.astype('int')
    data_raw.floor = data_raw.floor.astype('int')
    data_raw.floors = data_raw.floors.astype('int')
    data_raw.build_yr = data_raw.build_yr.astype('int')

    # adding custom column
    data_raw['price_of_sqm'] = data_raw.price / data_raw.area

    # cleaning outliers
    outliers_dict = dict()

    for column in ['area','price','price_of_sqm']:
        upper_quartile = np.nanpercentile(data_raw[column], 99.0)
        lower_quartile = np.nanpercentile(data_raw[column], 1.0)
        outliers_dict[column] = (lower_quartile, upper_quartile)

    outliers_dict['build_yr'] = (1900, pd.Timestamp.now().year)
    outliers_dict['floor'] = (0, 15)
    outliers_dict['floors'] = (0, 15)
    outliers_dict['rooms'] = (0, 6)
    outliers_dict['localization_x'] = (14,25)
    outliers_dict['localization_y'] = (48,55)

    for key in outliers_dict.keys():
        data_raw.drop(data_raw[data_raw[key] < outliers_dict[key][0]].index, inplace = True)
        data_raw.drop(data_raw[data_raw[key] > outliers_dict[key][1]].index, inplace = True)

    print(f'PIOTR: processed data shape: {data_raw.shape}')

    with engine.connect() as conn:
        data_raw.to_sql(con=conn, name='apt_details', if_exists='append', index=False,
        chunksize=30000)

    engine.dispose()

if __name__=='__main__':
    clean()