import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

def clean():

    query = '''
        select * from apt_details_raw
        where date = (select max(date) from apt_details_raw)
        '''

    username = 'piotr'
    password = os.environ['MYSQL_PASSWORD']
    host = 'mysql_apt_db'
    db_name = 'apt_db'
    db_url = f'mysql+mysqlconnector://{username}:{password}@{host}/{db_name}'

    engine = create_engine(db_url)

    with engine.connect() as conn:
        data_raw = pd.read_sql_query(sql=query, con=conn, index_col='id')

    # renaming market type
    data_raw.market.replace(['pierwotny','wtorny'], ['primary_market','aftermarket'], inplace=True)

    # removing foreign locations
    data_raw.drop(data_raw[data_raw.voivodeship=='zagranica'].index, inplace=True)

    # removing data with no price and changig data type
    data_raw.drop(index=data_raw[data_raw.price=='Zapytajoce'].index, axis=0, inplace=True)

    # remove records with missing data
    data_raw.dropna(subset=['price','localization_y','localization_x','area'], axis=0, inplace=True)
    data_raw.dropna(subset=['market','offer_type','rooms','floor'], axis=0, inplace=True)
    data_raw.dropna(subset=['build_yr'], axis=0, inplace=True)
    data_raw.dropna(subset=['floors'], axis=0, inplace=True)

    # cleaning categorical data and changing data type
    data_raw.rooms.replace('więcej niż 8', '8', inplace=True)

    data_raw.floor.replace(['niski parter','parter'], '0', inplace=True)
    data_raw.floor.replace('powyżej 30', '30', inplace=True)
    data_raw.drop(data_raw[data_raw.floor=='poddasze'].index, axis=0, inplace=True)

    data_raw.floors.replace(['0 (parter)','powyżej 30'], ['0','30'], inplace=True)

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

    for column in ['area','build_yr','price','price_of_sqm']:
        upper_quartile = np.nanpercentile(data_raw[column], 99.0)
        lower_quartile = np.nanpercentile(data_raw[column], 1.0)
        outliers_dict[column] = (lower_quartile, upper_quartile)

    outliers_dict['build_yr'] = (1900.0, 2024.0)
    outliers_dict['floor'] = (0, 15)
    outliers_dict['floors'] = (0, 15)
    outliers_dict['rooms'] = (0, 6)
    outliers_dict['localization_x'] = (14,25)
    outliers_dict['localization_y'] = (48,55)

    for key in outliers_dict.keys():
        data_raw.drop(data_raw[data_raw[key] < outliers_dict[key][0]].index, inplace = True)
        data_raw.drop(data_raw[data_raw[key] > outliers_dict[key][1]].index, inplace = True)

    # removing eaven location coordinates which have been identified as false
    data_raw.drop(data_raw[(data_raw.localization_x%1==0) | (data_raw.localization_y%1==0)].index, inplace=True)

    with engine.connect() as conn:
        data_raw.to_sql(con=conn, name='apt_details', if_exists='append', index=False)

    engine.dispose()

if __name__=='__main__':
    clean()