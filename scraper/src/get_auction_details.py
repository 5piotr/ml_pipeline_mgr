import os
import re
import datetime
import pytz
from bs4 import BeautifulSoup
from mysql import connector

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

voivodeships = [
'dolnośląskie',
'kujawsko-pomorskie',
'łódzkie',
'lubelskie',
'lubuskie',
'małopolskie',
'mazowieckie',
'opolskie',
'podkarpackie',
'podlaskie',
'pomorskie',
'śląskie',
'świętokrzyskie',
'warmińsko-mazurskie',
'warmińsko-mazurskie',
'wielkopolskie',
'zachodniopomorskie'
]

def get_current_timestamp():
    warsaw_tz = pytz.timezone('Europe/Warsaw') 
    timestamp = datetime.datetime.now(warsaw_tz).strftime('%Y-%m-%d_%H:%M:%S')
    return timestamp

def get_location(soup):
    try:
        location = soup.find_all('ul', class_="VZLIup")[0].find_all('li')
        if location[2].get_text() not in voivodeships:
            voivodeship = 'zagranica'
            city = location[3].get_text()
            district = location[2].get_text()

            return city, voivodeship, district

        voivodeship = location[2].get_text()
        if len(location)==4:
            city = location[3].get_text()
            district = None
        elif location[3].get_text().endswith('(pow.)'):
            city = location[4].get_text().split(' ')[0]
            district = None
        else:        
            city = location[3].get_text()
            prefixes = ['ul.','al.','Os.','pl.','rondo']
            counter = 0
            for prefix in prefixes:
                if location[4].get_text().startswith(prefix):
                    counter += 1
                    break
            if counter > 0:
                district = None
            else:
                district = location[4].get_text()
    except:
        voivodeship = None
        city = None
        district = None

    return city, voivodeship, district

def get_script(soup):
    try:
        script = soup.find('script', id='__NUXT_DATA__').get_text()
        script = script.replace('\\/','/').replace('\\/','/')
        script = script.encode('utf-8').decode('unicode_escape').encode('utf-8').decode('unicode_escape')
    except:
        script = []

    return script

def get_coordinates(script):
    try:
        result = re.findall(r',(\d{2}\.\d+),(\d{2}\.\d+),', script)[-1]
        localization_y = result[0]
        localization_x = result[1]
    except:
        localization_y = None
        localization_x = None

    return localization_x, localization_y

def get_market(script):
    try:
        market = re.findall(r'"rynek":"(\w+)', script)[-1]
    except:
        market = None 

    return market

def get_offer_type(script):
    try:
        offer_type = re.findall(r'"typoferty":"(\w+)', script)[-1]
    except:
        offer_type = None

    return offer_type

def get_area(script):
    try:
        area = re.findall(r'kowita","(\d*,?\d*)', script)[-1]
        area = area.replace(',','.')
    except:
        area = None

    return area

def get_rooms(script):
    try:
        rooms = re.findall(r'"number_of_rooms":(\d*)', script)[-1]
    except:
        rooms = None

    return rooms

def get_floors(soup):
    try:
        floo = soup.find_all('span', class_="Qa8Fuh")[2].get_text().split()[-1]
        if '/' in floo:
            li = floo.split('/')
            floor = li[0]
            floors = li[1]
        else:
            floor = floo
            floors = None
    except:
        floor = None
        floors = None

    return floor, floors

def get_buils_yr(script):
    try:
        build_yr = re.findall(r'"Rok budowy","(\d{4})', script)[-1]
    except:
        build_yr = None

    return build_yr

def get_price(script):
    try:
        price = re.findall(r'"price":(\d*),"priceCurrency', script)[-1]
    except:
        price = 'Zapytajoce'

    return price

def get_details(host='mysql_airflow_db'):

    query1 = '''
    select url 
    from apt_urls
    where date = (select max(date) from apt_urls)
    '''
    query2 = '''
    select max(date)
    from apt_urls
    '''

    with connector.connect(
        host = os.environ['PAPUGA_IP'],
        user = 'piotr',
        password = os.environ['MYSQL_PASSWORD'],
        database = 'airflow_db') as conn:
    
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
                chrome_options = Options()
                chrome_options.add_argument('--no-sandbox')
                chrome_options.add_argument('--headless')
                chrome_options.add_argument('--disable-dev-shm-usage')

                driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

                driver.get(url)

                wait = WebDriverWait(driver, timeout=10)
                element1 = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'Qa8Fuh')))
                element2 = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'VZLIup')))
                element3 = wait.until(EC.presence_of_element_located((By.ID, '__NUXT_DATA__')))

                page_source = driver.page_source
                soup = BeautifulSoup(page_source, "html.parser")

                driver.quit()
                
            except Exception as e:
                continue

            script = get_script(soup)

            city, voivodeship, district = get_location(soup)
            localization_x, localization_y = get_coordinates(script)
            market = get_market(script)
            offer_type = get_offer_type(script)
            area = get_area(script)
            rooms = get_rooms(script)
            floor, floors = get_floors(soup)
            build_yr = get_buils_yr(script)           
            price = get_price(script)

            record = (date, city, district, voivodeship, localization_y, localization_x, market,\
                    offer_type, area, rooms, floor, floors, build_yr, price, url)

            insert_single_record = f'''
            insert into apt_details_raw
            (date, city, district, voivodeship, localization_y, localization_x, market, offer_type,
                area, rooms, floor, floors, build_yr, price, url)
            values ({('%s,' * len(record))[:-1]})
            '''

            del city, district, voivodeship, localization_y, localization_x, market,\
                    offer_type, area, rooms, floor, floors, build_yr, price, url

            with conn.cursor() as cursor:
                cursor.execute(insert_single_record, record)
                conn.commit()

            i += 1
            if i%500 == 0 or i == len(auction_list):
                completion = round(i/len(auction_list)*100,2)
                timestamp = get_current_timestamp()
                print(f'PIOTR: list completion {completion}% at {timestamp}')

if __name__=='__main__':
    get_details(host='localhost')
