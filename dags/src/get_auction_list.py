import os
import requests
from mysql import connector
from bs4 import BeautifulSoup
from .lib import get_current_timestamp

FLAT_SIZE = [[0,30],[30,35],[35,37],[37,40],[40,42],[42,45],[45,47],[47,50],
             [50,52],[52,55],[55,57],[57,60],[60,62],[62,65],[65,70],[70,75],
             [75,80],[80,90],[90,100],[100,120],[120,1000]]

def get_list(host='mysql_airflow_db',
             flat_size=FLAT_SIZE):

    timestamp = get_current_timestamp()
    auction_list = []

    # iterating over flat size ranges
    for mi,ma in flat_size:
        url = f'https://gratka.pl/nieruchomosci/mieszkania?powierzchnia-w-m2:min={mi}&powierzchnia-w-m2:max={ma}'
        page = requests.get(url, timeout=30)
        soup = BeautifulSoup(page.content, 'html.parser')
        pagination = soup.find_all(class_="pagination__input")

        # iterating over result pages
        for i in range(int(pagination[0]["max"])):
            url = f'https://gratka.pl/nieruchomosci/mieszkania?page={i+1}&powierzchnia-w-m2:min={mi}&powierzchnia-w-m2:max={ma}'
            page = requests.get(url, timeout=30)
            soup = BeautifulSoup(page.content, 'html.parser')
            links = soup.find_all(class_="teaserUnified")

            # iterating over apartment links
            for link in links:
                auction_list.append((timestamp, link['data-href']))

        print(f'PIOTR: mi={mi} ma={ma} complete, auction list has now {len(auction_list)} urls')

    auction_list = list(set(auction_list))
    print(f'PIOTR: duplicates removed, list has now {len(auction_list)} urls')

    insert_multiple_records = "INSERT INTO apt_urls (date, url) VALUES (%s, %s)"

    with connector.connect(
        host = host,
        user = 'piotr',
        password = os.environ['MYSQL_PASSWORD'],
        database = 'airflow_db'
    ) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(insert_multiple_records, auction_list)
            conn.commit()

if __name__=='__main__':
    get_list(host='localhost',
             flat_size=[[0,15]])
