import pickle as pkl
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
from tensorflow import keras

import lib

x_train, x_test, y_train, y_test = lib.load_train_test()

scaler = MinMaxScaler()
x_train= scaler.fit_transform(x_train)
x_test = scaler.transform(x_test)

with open('/models/scaler.pkl','wb') as file:
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

model.fit(x=x_train,
          y=y_train,
          validation_split=0.1,
          batch_size=256,
          epochs=10,
          verbose=0)

model.save('/models/ann.keras')

predictions = model.predict(x_test, verbose=0)

r2 = lib.evaluate_pred(y_test, predictions)
