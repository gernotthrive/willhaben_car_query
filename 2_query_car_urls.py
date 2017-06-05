import sys
import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO) #filename='2_query_car_urls.log', filemode='a')

logging.info('Starting script')

from bs4 import BeautifulSoup
from bs4 import SoupStrainer

from urllib.request import urlopen
import re
import datetime, time

import sqlite3

def download_make(make_id,area):
    ## Querying a single make
    singleurl = 'https://www.willhaben.at/iad/gebrauchtwagen/auto/gebrauchtwagenboerse?areaId='+str(area)+'&CAR_MODEL/MAKE='+str(make_id)
    url = singleurl
    logging.info('URL: %s', url)

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

        # check how many cars are in the page
        if pagecarcount > 0:
            # extract the car links
            for car in carlist:
                link=car.find('a')
                if link:
                    carcount += 1
                    carurl = {}
                    href=link['href']

                    carurl['url'] = 'https://www.willhaben.at'+href
                    carurl['id'] = make_id
                    now = datetime.datetime.now()
                    carurl['now'] = now
                    ## update or create entry
                    c.execute('SELECT * FROM willhaben_car_urls WHERE url=?',(carurl['url'],))

                    if (c.fetchone()):
                        c.execute('UPDATE willhaben_car_urls SET last_updated=? WHERE url=?',(now, carurl['url']))
                        logging.info('Updated id/url: %s/%s',carurl['id'],carurl['url'])
                        update_count += 1
                    else:
                        c.execute('INSERT INTO willhaben_car_urls (id, url, last_updated) VALUES (:id, :url, :now)',carurl)
                        logging.info('Inserted id/url: %s/%s',carurl['id'],carurl['url'])
                        insert_count += 1

        logging.info('# Cars: '+str(carcount))
        conn.commit()
        logging.info('Changes committed to DB')

        pagecount += 1
        url = singleurl + '&page=' + str(pagecount)

        ## no cars found --> we are done here
        if (pagecarcount == 0):
            found_cars=False

        ## Willhaben doesn't do more than 100 pages
        if pagecount > 100:
            found_cars=False

    # to get the real pagecount, last one was empty and iterator is at the end --> minus 2
    if (pagecount >1):
        pagecount -= 2

    logging.info('Finished. %i cars on %i pages. Inserted: %i || Updated: %i)',carcount, pagecount, insert_count, update_count)


conn = sqlite3.connect('willhaben_cars.db')
c = conn.cursor()
logging.info('DB connected')

## loop through list of makes
rows = c.execute('SELECT id, name, last_area FROM willhaben_car_makes ORDER BY last_area ASC, last_update ASC')
rows = rows.fetchall()
for row in rows:
    make_id = row[0]

    ## too many cars so let's download area by area
    last_area = row[2]
    area = 0

    if last_area:
        if last_area == 9:
            area = 1
        else:
            area = int(last_area)+1
    else:
        area = 1

    # let's pick up where we left off
    for curr_area in range(area, 10):
        logging.info('Downloading ID %s for area %i | %s',make_id,curr_area, row[1])
        download_make(make_id, curr_area)
        now = datetime.datetime.now()
        c.execute('UPDATE willhaben_car_makes SET last_update=?, last_area=? WHERE id=?',(now,curr_area, make_id))
        conn.commit()


logging.info('Finished downloading all URLs')
conn.close()
