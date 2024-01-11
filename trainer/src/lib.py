import pickle as pkl
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error ,r2_score

def load_train_test():

    with open('/code/train_data/x_train.pkl','rb') as file:
        x_train = pkl.load(file)

    with open('/code/train_data/y_train.pkl','rb') as file:
        y_train = pkl.load(file)

    with open('/code/train_data/x_test.pkl','rb') as file:
        x_test = pkl.load(file)

    with open('/code/train_data/y_test.pkl','rb') as file:
        y_test = pkl.load(file)

    for frame in [x_train,x_test]:
        frame.drop('date', axis=1, inplace=True)

    return x_train, x_test, y_train, y_test

def evaluate_pred(y_test, predictions):
    mae = round(mean_absolute_error(y_test, predictions))
    mse = round(np.sqrt(mean_squared_error(y_test, predictions)))
    r2 = round(r2_score(y_test, predictions),2)
    print(f'PIOTR: MAE {mae}, MSE {mse}, R^2 {r2}')
    return r2
