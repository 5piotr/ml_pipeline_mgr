import pandas as pd
from sklearn.model_selection import train_test_split
import lib

# load data
data = lib.load_pkl('train_data/apt_details_cls.pkl')

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
lib.save_pkl(x_train, 'train_data/x_train.pkl')
lib.save_pkl(x_test, 'train_data/x_test.pkl')
lib.save_pkl(y_train, 'train_data/y_train.pkl')
lib.save_pkl(y_test, 'train_data/y_test.pkl')

# preparing frame for predictions
pred_frame = x_train.iloc[:1,:].copy()
pred_frame.drop(columns='date', inplace=True)
pred_frame.replace(pred_frame.iloc[0], 0, inplace=True)

lib.save_pkl(pred_frame, '../models/temp/pred_frame.pkl')
