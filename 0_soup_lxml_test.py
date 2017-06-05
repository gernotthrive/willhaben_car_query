import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO) #filename='2_query_car_urls.log', filemode='a')

logging.info('Starting script')

from bs4 import BeautifulSoup
from urllib.request import urlopen
import re
import datetime, time

import sqlite3
conn = sqlite3.connect('willhaben_cars.db')
c = conn.cursor()
logging.info('DB connected')

def download_make(make_id):
    ## Querying a single make
    singleurl = 'https://www.willhaben.at/iad/gebrauchtwagen/auto/gebrauchtwagenboerse?CAR_MODEL/MAKE='+str(make_id)
    url = singleurl

    found_cars=True
    pagecount=1
    carcount=0

    update_count = 0
    insert_count = 0

    ## loop over all possibles pages until we find one without cars / 404-ish
    while (found_cars):
        pagecarcount = 0

        logging.info('Page # '+str(pagecount))

        html = urlopen(url)
        soup = BeautifulSoup(html, "lxml")
        carlist = soup.find_all('article')

        pagecarcount = len(carlist)
        logging.info('# Cars: '+str(pagecarcount))
        # check how many cars are in the page
        if pagecarcount > 0:
            # extract the car links
            for car in carlist:
                link=car.find('a')
                if link:
                    carcount += 1
                    carurl = {}
                    carurl['url'] = 'https://www.willhaben.at'+link['href']
                    carurl['id'] = make_id
                    now = datetime.datetime.now()
                    carurl['now'] = now
                    ## update or create entry
                    c.execute('SELECT * FROM willhaben_car_urls WHERE url=?',(carurl['url'],))

                    if (c.fetchone()):
                        logging.info('Updated id/url: %s/%s',carurl['id'],carurl['url'])
                        update_count += 1
                    else:
                        logging.info('Inserted id/url: %s/%s',carurl['id'],carurl['url'])
                        insert_count += 1
        pagecount += 1
        url = singleurl + '&page=' + str(pagecount)

        if (pagecarcount == 0):
            found_cars=False

    # to get the real pagecount, last one was empty and iterator is at the end --> minus 2
    if (pagecount >1):
        pagecount -= 2
    logging.info('Finished. %i cars on %i pages. Inserted: %i || Updated: %i)',carcount, pagecount, insert_count, update_count)



make_id = 1007
logging.info('Downloading ID %s',make_id)
download_make(make_id)
now = datetime.datetime.now()
logging.info('Changes committed to DB')

logging.info('Finished downloading all URLs')
conn.close()
