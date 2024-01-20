from xgboost import XGBRegressor
import lib

x_train, x_test, y_train, y_test = lib.load_train_test('train_data/')

model = XGBRegressor(objective='reg:squarederror',
                     n_estimators=500,
                     tree_method='hist',
                     random_state=42)

model.fit(X=x_train,
          y=y_train,
          verbose=True)

model.save_model('../models/temp/xgb.json')

predictions = model.predict(x_test)

r2 = lib.evaluate_pred(y_test, predictions)

lib.save_txt(str(r2), '../models/temp/xgb.r2')
