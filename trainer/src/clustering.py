import os
from sqlalchemy import create_engine
import pandas as pd
from sklearn.cluster import KMeans

query = '''
    select * from apt_details
    where date = (select max(date) from apt_details)
    '''

username = 'piotr'
password = os.environ['MYSQL_PASSWORD']
host = os.environ['PAPUGA_IP']
db_name = 'apt_db'
db_url = f'mysql+mysqlconnector://{username}:{password}@{host}/{db_name}'

engine = create_engine(db_url)

with engine.connect() as conn:
    data = pd.read_sql_query(sql=query, con=conn, index_col='id')

X = data.loc[:,['localization_y','localization_x']].values
kmeans = KMeans(n_clusters = 600, n_init='auto', random_state = 0).fit(X)
data['cluster'] = kmeans.labels_

with engine.connect() as conn:
    data.to_sql(con=conn, name='apt_details_cls', if_exists='replace', index=False)

engine.dispose()
