import os
import pickle as pkl
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sklearn.metrics import mean_squared_error, mean_absolute_error ,r2_score

def load_pkl(path):
    with open(path,'rb') as file:
        data = pkl.load(file)
    return data

def save_pkl(data, path):
    with open(path,'wb') as file:
        pkl.dump(data, file)

def save_txt(txt, path):
    with open(path,'w') as file:
        file.write(txt)

def get_df_from_mysql(query):
    try:
        username = 'piotr'
        password = os.environ['MYSQL_PASSWORD']
        host = os.environ['PAPUGA_IP']
        db_name = 'apt_db'
        db_url = f'mysql+mysqlconnector://{username}:{password}@{host}/{db_name}'

        engine = create_engine(db_url)
        with engine.connect() as conn:
            data = pd.read_sql_query(sql=query, con=conn, index_col='id')

        return data
    
    except Exception as e:
        print(e)
        raise Exception
    
    finally:
        if engine:
            engine.dispose()

def load_train_test(paht):

    x_train = load_pkl(f'{paht}x_train.pkl')
    y_train = load_pkl(f'{paht}y_train.pkl')
    x_test = load_pkl(f'{paht}x_test.pkl')
    y_test = load_pkl(f'{paht}y_test.pkl')

    for frame in [x_train,x_test]:
        frame.drop('date', axis=1, inplace=True)

    return x_train, x_test, y_train, y_test

def evaluate_pred(y_test, predictions):
    mae = round(mean_absolute_error(y_test, predictions))
    mse = round(np.sqrt(mean_squared_error(y_test, predictions)))
    r2 = round(r2_score(y_test, predictions),2)
    print(f'PIOTR: MAE {mae}, MSE {mse}, R^2 {r2}')
    return r2
