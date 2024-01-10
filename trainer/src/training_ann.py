import pickle as pkl
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
from tensorflow import keras
from sklearn.metrics import mean_squared_error, mean_absolute_error ,r2_score

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

for frame in [x_train,y_train,x_test,y_test]:
    frame = frame.values

scaler = MinMaxScaler()
x_train= scaler.fit_transform(x_train)
x_test = scaler.transform(x_test)

with open('models/scaler.pkl','wb') as file:
    pkl.dump(scaler, file)

def create_model():
    model = tf.keras.Sequential([
        keras.layers.Dense(32, activation=tf.keras.layers.LeakyReLU(alpha=0.01),
                           input_dim=x_test.shape[1]),
        keras.layers.Dense(16, activation=tf.keras.layers.LeakyReLU(alpha=0.01)),
        keras.layers.Dense(8, activation=tf.keras.layers.LeakyReLU(alpha=0.01)),
        keras.layers.Dense(4, activation=tf.keras.layers.LeakyReLU(alpha=0.01)),
        keras.layers.Dense(1)
    ])
    model.compile(optimizer='adam', loss='mean_squared_error')
    return model

model = create_model()

model.summary()

model.fit(x=x_train,
          y=y_train,
          validation_split=0.1,
          batch_size=256,
          epochs=10,
          verbose=0)

model.save('/code/models/ann.keras')

predictions = model.predict(x_test, verbose=0)

mae = round(mean_absolute_error(y_test, predictions))
mse = round(np.sqrt(mean_squared_error(y_test, predictions)))
r2 = round(r2_score(y_test, predictions),2)
print(f'PIOTR: MAE {mae}, MSE {mse}, R^2 {r2}')
