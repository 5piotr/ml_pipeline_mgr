import os
import requests
import re
import datetime
import pytz
from bs4 import BeautifulSoup
from mysql import connector

def get_location(soup):
    try:
        location = soup.find_all('span',class_="offerLocation__region")
    except:
        return None, None, None
    
    try:
        city = location[0].text.strip().replace(',','')
    except:
        city = None
    
    try:
        voivodeship = location[-1].text.strip().replace(',','')
    except:
        voivodeship = None

    if len(location) == 3:
        try:
            district = location[1].text.strip().replace(',','')
        except:
            district = None
    else:
        district = None

    return city, voivodeship, district

def get_coordinates(soup):
    try:
        scripts = soup.find(id='leftColumn').find_all('script')
    except:
        return None, None
    
    try:
        localization_y = re.findall(r'(\"\w+szerokosc.*?\"):(-?\d*[.\d]*)', scripts[4].contents[0])[0][1]
    except:
        try:
            localization_y = re.findall(r'(\"\w+szerokosc.*?\"):(-?\d*[.\d]*)', scripts[5].contents[0])[0][1]
        except:
            localization_y = None

    try:
        localization_x = re.findall(r'(\"\w+dlugosc.*?\"):(-?\d*[.\d]*)', scripts[4].contents[0])[0][1]
    except:
        try:
            localization_x = re.findall(r'(\"\w+dlugosc.*?\"):(-?\d*[.\d]*)', scripts[5].contents[0])[0][1]
        except:
            localization_x = None

    return localization_x, localization_y

def get_market_offer(soup):
    try:
        scripts = soup.find('body').find_all('script')  
    except:
        return None, None

    try:
        market = re.findall(r'(\"rynek\"):\"(.*?)\"', scripts[18].contents[0])[0][1]
    except:
        market = None

    try:
        offer_type = re.findall(r'(\"typ_oferty\"):\"(.*?)\"', scripts[18].contents[0])[0][1]
    except:
        offer_type = None

    return market, offer_type

def get_parameters(soup):
    try:
        parameters = soup.find_all('ul',class_="parameters__singleParameters")
    except:
        return None, None, None, None, None
    
    try:
        area = re.findall(r'(Powierzchnia).*\n(.*)m', parameters[0].text)
        area = area[0][1].replace(' ','')
        area = area.replace(',','.')
    except:
        area = None

    try:
        rooms = re.findall(r'(Liczba pokoi).*\n(.*)', parameters[0].text)
        rooms = rooms[0][1]
    except:
        rooms = None

    try:
        floor = re.findall(r'(Piętro).*\n(.*)', parameters[0].text)
        floor = floor[0][1]
    except:
        floor = None

    try:
        floors = re.findall(r'(Liczba pięter w budynku).*\n(.*)', parameters[0].text)
        floors = floors[0][1]
    except:
        floors = None

    try:
        build_yr = re.findall(r'(Rok budowy).*\n(.*)', parameters[0].text)
        build_yr = build_yr[0][1]
    except:
        build_yr = None

    return area, rooms, floor, floors, build_yr

def get_price(soup):
    try:
        price_info = soup.find_all('span',class_="priceInfo__value")
        price = price_info[0].text.replace(' ','').replace('\n','').replace(',','.')[:-2]
    except:
        price = None

    return price

def get_details():

    query1 = '''
    select link 
    from apt_links
    where date = (select max(date) from apt_links)
    '''
    query2 = '''
    select max(date)
    from apt_links
    '''

    with connector.connect(
        host = 'mysql_apt_db',
        user = 'piotr',
        password = os.environ['MYSQL_PASSWORD'],
        database = 'apt_db'
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query1)
            result1 = cursor.fetchall()
            cursor.execute(query2)
            result2 = cursor.fetchall()

    auction_list = [row[0] for row in result1]
    date = result2[0][0]

    i = 0

    for url in auction_list:
        try:
            page = requests.get(url)
            soup = BeautifulSoup(page.text, 'html.parser')
        except:
            continue

        city, voivodeship, district = get_location(soup)
        localization_x, localization_y = get_coordinates(soup)
        market, offer_type = get_market_offer(soup)
        area, rooms, floor, floors, build_yr = get_parameters(soup)
        price = get_price(soup)

        insert_single_record = '''
        INSERT INTO apt_details_raw
        (date, city, district, voivodeship, localization_y, localization_x, market, offer_type,
            area, rooms, floor, floors, build_yr, price, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        record = (date, city, district, voivodeship, localization_y, localization_x, market,\
                  offer_type, area, rooms, floor, floors, build_yr, price, url)

        with connector.connect(
            host = 'mysql_apt_db',
            user = 'piotr',
            password = os.environ['MYSQL_PASSWORD'],
            database = 'apt_db'
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(insert_single_record, record)
                conn.commit()

        i += 1
        if i%500 == 0 or i == len(auction_list):
            completion = round(i/len(auction_list)*100,2)
            warsaw_tz = pytz.timezone('Europe/Warsaw') 
            timestamp = datetime.datetime.now(warsaw_tz).strftime('%H:%M:%S')
            print(f'PIOTR: list completion {completion}% at {timestamp}')

if __name__=='__main__':
    get_details()
