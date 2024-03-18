import os
from mysql import connector

def email():

    query = '''
    select auction_urls, data_raw, data_clean, ann_r2, xgb_r2, prod
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

    try:
        auction_urls = result[0][0]
        data_raw = result[0][1]
        data_clean = result[0][2]
        ann_r2 = result[0][3]
        xgb_r2 = result[0][4]
        prod = result[0][5]
    except:
        return -1

    email = f'''
    apt_price_estimator finished running:
    <ul>
    <li>auction urls: {auction_urls}</li>
    <li>data_raw: {data_raw}</li>
    <li>data_clean: {data_clean}</li>
    <li>ann_r2: {ann_r2}</li>
    <li>xgb_r2: {xgb_r2}</li>
    <li>prod: {prod}</li>
    </ul>
    '''

    return email

if __name__=='__main__':
    print(email())
