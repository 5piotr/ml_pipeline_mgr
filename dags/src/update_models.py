import os
import shutil
from mysql import connector
from .lib import get_current_timestamp

def update():

    query = '''
    select prod
    from apt_log
    where date = (select max(date) from apt_log)
    '''

    with connector.connect(
        host = 'mysql_airflow_db',
        user = 'piotr',
        password = os.environ['MYSQL_PASSWORD'],
        database = 'airflow_db') as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()

    prod = result[0][0]

    timestamp = get_current_timestamp()

    if prod==1:
        with open('/models/update.date', 'w') as file:
            file.write(timestamp)

        for model in ['ann.keras','kmeans.pkl','pred_frame.pkl','scaler.pkl','xgb.json']:
            shutil.copy2(f'/models/temp/{model}', '/models')

if __name__=='__main__':
    update()
