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
import json

#########################################################################################
## helper function to convert a number out of HTML into a real typed number
#########################################################################################
def convert_decimalpointnumber(numbrstr):

    ## normal typecast
    try:
        numbr=int(numbrstr)
        return numbr

    ## if that doesn't work - let's get into it!
    except:
        numbrlist = re.findall('\d+',numbrstr)       # find all numbers in the string
        if (numbrlist):
            numbr = int(''.join(numbrlist))              # join em together & typecast
            return numbr
        else:
            return 0

#########################################################################################
## helper function to extract content from a HTML container
#########################################################################################
def get_content_str(bs4_obj):
    str_con=''

    if (bs4_obj):
        for string in bs4_obj.stripped_strings:
            str_con=str_con+repr(string+' ')
        str_clean = str_con.replace('\'','')
        return str_clean
    else:
        return ''

#########################################################################################
## download a full willhaben car url
#########################################################################################
def parse_willhaben_url(url):
    html = urlopen(url)
    soup = BeautifulSoup(html, "html.parser")
    line={}

    ## PRICE
    pricediv = soup.find("div", { "class" : "price" })
    # parsing the price is hard as it's stored a bit complicated .. let's find all numbers
    line['Preis'] = 0
    line['DEBUG_Preis']=''
    priceint = 0

    if (pricediv):
        priceint=convert_decimalpointnumber(pricediv.string.strip())
        line['Preis']=priceint

        # for debug reasons we will store a STR version too
        line['DEBUG_Preis']=pricediv.string.strip()

    ##################################################
    ## IF WE FOUND NO PRICE, ABORT!
    ##################################################

    if (priceint<=0):
        return None

    ##################################################
    ## IF WE FOUND A PRICE, LET'S RUMBLE!!!
    ##################################################

    ## LINK
    line['Link']=url

    ## Site title
    titlediv = soup.find("h1", { "class" : "header" })

    line['Titel']=get_content_str(titlediv)

    ## RESTLICHE SEITE AUSLESEN
    car_table = soup.find("div", { "class" : "car-data" })
    if (car_table):

        ## FAHRZEUGDATEN BOX
        # this information we need for sure:
        line['Kilometerstand'] = ''
        line['Erstzulassung'] = ''
        line['Leistung_kW'] = ''
        line['Treibstoff'] = ''
        line['Getriebeart'] = ''
        line['Antrieb'] = ''
        line['Farbe'] = ''
        line['Sitze'] = ''
        line['Tueren'] = ''

        fahrzeugdaten={}
        rows = car_table.findAll("div", {"class" : "col-xs-6" })

        for single_row in rows:
            title=single_row.find("span", {"class":"col-2-desc"})
            value=single_row.find("div", {"class":"col-2-body"})

            strtitle=title.string
            # as the table is not formatted straight away
            # we only take the first string for values, title is always clear
            values = []
            for string in value.stripped_strings:
                values.append(repr(string))
            strvalue=values[0]
            strvalue_clean = strvalue.replace('\'','')

            fahrzeugdaten[strtitle]=strvalue_clean

            ## now let's digest it into relational

            if (strtitle == 'Kilometerstand'):
                line['Kilometerstand']=convert_decimalpointnumber(strvalue_clean)
            elif (strtitle == 'Erstzulassung'):
                line['Erstzulassung']=strvalue_clean
            elif (strtitle == 'Leistung (kW)'):
                line['Leistung_kW']=convert_decimalpointnumber(strvalue_clean)
            elif (strtitle == 'Treibstoff'):
                line['Treibstoff']=strvalue_clean
            elif (strtitle == 'Getriebeart'):
                line['Getriebeart']=strvalue_clean
            elif (strtitle == 'Antrieb'):
                line['Antrieb']=strvalue_clean
            elif (strtitle == 'Außenfarbe'):
                line['Farbe']=strvalue_clean
            elif (strtitle == 'Anzahl Sitze'):
                line['Sitze']=int(strvalue_clean)
            elif (strtitle == 'Anzahl Türen'):
                line['Tueren']=int(strvalue_clean)

        line['FULL_Fahrzeugdaten'] = json.dumps(fahrzeugdaten)

        ## GETTING DESCRIPTION BOX FULL
        descriptiontable = soup.find("div", { "class" : "description" })
        line['FULL_Beschreibung']=get_content_str(descriptiontable)

        ## AUSSTATTUNG BOX
        line['FULL_Ausstattung']=''
        ausstattungcontainer = soup.find_all("ul", { "class" : "eq-list" })
        if (ausstattungcontainer):
            ausstattungstr=''
            for ausstattungtable in ausstattungcontainer:
                for string in ausstattungtable.strings:
                    ausstattungstr=ausstattungstr+string.strip()+' '
            line['FULL_Ausstattung']=ausstattungstr

        ## VERKÄUFER BOX
        line['FULL_Kontakt'] = ''
        contactouter = soup.find("div", { "class" : "contact-desc" })
        if (contactouter):
            contactcontainer = contactouter.find("dl", {"class" : "dl-horizontal"})
            contactstr=contactcontainer.stripped_string
            line['FULL_Kontakt']=contactstr

        now = datetime.datetime.now()
        line['last_updated']=now

    return line

#########################################################################################
## main code
#########################################################################################
conn = sqlite3.connect('willhaben_cars.db')
c = conn.cursor()
logging.info('DB connected')

rows = c.execute('select id, url from willhaben_car_urls ORDER BY last_updated ASC LIMIT 0,100;')
rows = rows.fetchall()

for row in rows:
    car_id=row[0]
    url=row[1]
    now = datetime.datetime.now()
    logging.info('Downloading ID %s | URL %s',car_id,url)
    line=parse_willhaben_url(url)
    if (line):
        logging.info('Car Data found & stored!')
        #found ze car data
        c.execute('''INSERT INTO willhaben_cars (Titel, Antrieb, Erstzulassung, Farbe, Getriebeart, Kilometerstand, Leistung_kW, Link, Preis, Sitze, Treibstoff, Tueren, DEBUG_Preis, FULL_Ausstattung, FULL_Beschreibung, FULL_Fahrzeugdaten, FULL_Kontakt, last_updated)
                     VALUES (:Titel, :Antrieb, :Erstzulassung, :Farbe, :Getriebeart, :Kilometerstand, :Leistung_kW, :Link, :Preis, :Sitze, :Treibstoff, :Tueren, :DEBUG_Preis, :FULL_Ausstattung, :FULL_Beschreibung, :FULL_Fahrzeugdaten, :FULL_Kontakt, :last_updated)''', line)
        c.execute('UPDATE willhaben_car_urls SET last_updated=?, outdated=0 WHERE url=?',(now,url))
        conn.commit()
    else:
        # flag the URL as 404
        # update URL data
        logging.info('Car Data not found, invalidating URL!')
        c.execute('UPDATE willhaben_car_urls SET last_updated=?, outdated=1 WHERE url=?',(now,url))
        conn.commit()

logging.info('Finished downloading all URLs')
conn.close()
