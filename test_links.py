from bs4 import BeautifulSoup
import requests
import time
import re
import numpy as np
import pandas as pd
from tqdm import tqdm
import my_lib
import datetime
from mysql import connector
import os

flat_size = [[0,10]]

start = time.time()

timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
auction_list = []

for mi,ma in flat_size:
    url = f'https://gratka.pl/nieruchomosci/mieszkania?powierzchnia-w-m2:min={mi}&powierzchnia-w-m2:max={ma}'
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    pagination = soup.find_all(class_="pagination__input")
    for i in range(int(pagination[0]["max"])):
        url = f'https://gratka.pl/nieruchomosci/mieszkania?page={i+1}&powierzchnia-w-m2:min={mi}&powierzchnia-w-m2:max={ma}'
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        links = soup.find_all(class_="teaserUnified")
        for link in links:
            auction_list.append((timestamp, link['data-href']))
    print(f'mi={mi} ma={ma} complete, auction list has now {len(auction_list)} links')
        
stop = time.time()
print(round((stop-start)/60,1),'min.')

insert_single_record = "INSERT INTO apt_links (date, link) VALUES (%s, %s)"

try:
    with connector.connect(
        host = "localhost",
        user = "piotr",
        password = os.environ['MYSQL_PASSWORD'],
        database = "apt_db"
    ) as database:
        print(database)
        with database.cursor() as cursor:
            cursor.executemany(insert_single_record, auction_list)
            database.commit()
except connector.Error as e:
    print(e)

