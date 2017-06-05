import sys
import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename='1_query_make_list.log', filemode='a', level=logging.INFO)

logging.info('Starting script')

from bs4 import BeautifulSoup
from urllib.request import urlopen
import re
import datetime, time

import sqlite3
conn = sqlite3.connect('willhaben_cars.db')
c = conn.cursor()
logging.info('DB connected')

url = 'https://www.willhaben.at/iad/gebrauchtwagen/'

now = datetime.datetime.now()

html = urlopen(url)
soup = BeautifulSoup(html, "lxml")
logging.info('HTML downloaded')

carlistselect = soup.find('select', {'id' : 'fast-search-car-model-make'})
links = carlistselect.find_all('option')
link_count = len(links)

logging.info(str(link_count)+' car makes found')

update_count = 0
insert_count = 0


    for link in links:
        try:
            if (link['value']):
                car={}
                car['id']=link['value']
                car['name']=link.string
                car['link']='https://www.willhaben.at/iad/gebrauchtwagen/auto/gebrauchtwagenboerse?CAR_MODEL/MAKE='+link['value']
                car['last_update']=now

                # if ID already exists, just update
                car_id = (car['id'], )
                c.execute('SELECT * FROM willhaben_car_makes WHERE id=?',car_id)

                if (c.fetchone()):
                    c.execute('UPDATE willhaben_car_makes SET name=:name, link=:link, last_update=:last_update WHERE id=:id',car)
                    logging.info('Updated id/car: '+car['id']+'/'+car['name'])
                    update_count += 1
                else:
                    c.execute('INSERT INTO willhaben_car_makes VALUES (:id,:name,:link, :last_update)',car)
                    logging.info('Inserted id/car: '+car['id']+'/'+car['name'])
                    insert_count += 1
        except:
            logging.error('Unexpected error: '+sys.exc_info()[0])

conn.commit()
conn.close()

logging.info('Finished script, '+str(insert_count)+' inserted & '+str(update_count)+' updated')
