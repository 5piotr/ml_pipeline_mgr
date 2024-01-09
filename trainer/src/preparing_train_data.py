import os
from sqlalchemy import create_engine
import pandas as pd
import pickle as pkl
from sklearn.model_selection import train_test_split

query = '''
    select * from apt_details_cls
    '''

username = 'piotr'
password = os.environ['MYSQL_PASSWORD']
host = os.environ['PAPUGA_IP']
db_name = 'apt_db'
db_url = f'mysql+mysqlconnector://{username}:{password}@{host}/{db_name}'

engine = create_engine(db_url)
with engine.connect() as conn:
    data = pd.read_sql_query(sql=query, con=conn)
engine.dispose()

# removing unnecessary columns
to_drop = ['city','district','voivodeship','localization_y','localization_x','price_of_sqm','url','offer_type']
data.drop(to_drop, axis=1, inplace=True)

# creating dummy variables
for column in ['rooms','floor','floors','cluster']:
    data[column] = data[column].astype('str')

data = pd.get_dummies(data, drop_first=False, dtype='uint8')

# removing first dummy columns
to_drop = ['market_aftermarket','rooms_1','floor_0','floors_0','cluster_0']
data.drop(to_drop, axis=1, inplace=True)

# train test split
x = data.drop('price',axis=1)
y = data.price

x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2)


# save data
with open('train_data/x_train.pkl','wb') as file:
    pkl.dump(x_train, file)

with open('train_data/x_test.pkl','wb') as file:
    pkl.dump(x_test, file)

with open('train_data/y_train.pkl','wb') as file:
    pkl.dump(y_train, file)

with open('train_data/y_test.pkl','wb') as file:
    pkl.dump(y_test, file)
