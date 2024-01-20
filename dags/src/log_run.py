import os
from mysql import connector

def get_value_from_query(query, conn):
    with conn.cursor() as cursor:
        cursor.execute(query)
        result1 = cursor.fetchall()
    return result1[0][0]

def get_value_from_file(file_path):
    with open(file_path, 'r') as file:
        value = file.read()
    return float(value)

def log():

    with connector.connect(
        host = 'mysql_apt_db',
        user = 'piotr',
        password = os.environ['MYSQL_PASSWORD'],
        database = 'apt_db') as conn:

        query1 = '''
        select max(date)
        from apt_urls
        '''
        date = get_value_from_query(query=query1, conn=conn)

        query2 = f'''
        select count(url)
        from apt_urls
        where date = '{date}'
        '''
        auction_urls = get_value_from_query(query=query2, conn=conn)

        query3 = f'''
        select count(url)
        from apt_details_raw
        where date = '{date}'
        '''
        data_raw = get_value_from_query(query=query3, conn=conn)

        query4 = f'''
        select count(url)
        from apt_details
        where date = '{date}'
        '''
        data_clean = get_value_from_query(query=query4, conn=conn)

        ann_r2 = get_value_from_file('/models/temp/ann.r2')
        xgb_r2 = get_value_from_file('/models/temp/xgb.r2')

        if ann_r2>=0.78 and xgb_r2>=0.78:
            prod = 1
        else:
            prod = 0

        record = (date, auction_urls, data_raw, data_clean, ann_r2, xgb_r2, prod)

        insert_single_record = f'''
        insert into apt_log
        (date, auction_urls, data_raw, data_clean, ann_r2, xgb_r2, prod)
        values ({('%s,' * len(record))[:-1]})
        '''
        
        with conn.cursor() as cursor:
            cursor.execute(insert_single_record, record)
            conn.commit()

if __name__=='__main__':
    log()
