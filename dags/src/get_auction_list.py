import os
import requests
from mysql import connector
from bs4 import BeautifulSoup
from .lib import get_current_timestamp

FLAT_SIZE = [[0, 25]] \
            + [[25, 30]] \
            + [[a, a+1] for a in range(30, 40, 1)] \
            + [[a, a+2] for a in range(40, 70, 2)] \
            + [[a, a+4] for a in range(70, 100, 4)] \
            + [[100, 150]] \
            + [[151, 1000]]

def get_list(host='mysql_airflow_db',
             flat_size=FLAT_SIZE):

    timestamp = get_current_timestamp()
    auction_list = []

    # iterating over flat size ranges
    for mi,ma in flat_size:
        url = f'https://gratka.pl/nieruchomosci/mieszkania?location[map]=1&location[map_bounds]=55.0,24.0:49.0,14.0&powierzchnia-w-m2:max={ma}&powierzchnia-w-m2:min={mi}&sort=relevance'
        page = requests.get(url, timeout=30)
        soup = BeautifulSoup(page.content, 'html.parser')
        result = soup.find_all(class_="pagination__item")
        pages_count = result[-1].text

        # iterating over result pages
        for i in range(int(pages_count)):
            url = f'https://gratka.pl/nieruchomosci/mieszkania?page={i+1}&location[map]=1&location[map_bounds]=55.0,24.0:49.0,14.0&powierzchnia-w-m2:max={ma}&powierzchnia-w-m2:min={mi}&sort=relevance'
            page = requests.get(url, timeout=30)
            soup = BeautifulSoup(page.content, 'html.parser')
            links = soup.find_all(class_="card__outer")

            # iterating over apartment links
            for link in links:
                auction_list.append((timestamp, f'https://gratka.pl{link.a.get("href")}'))

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
             flat_size=[[0,25]])
