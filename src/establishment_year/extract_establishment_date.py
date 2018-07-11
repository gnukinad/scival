import logging
import sys
import os
import pandas as pd
import numpy as np
import selenium
import re
sys.path.append('..')
from db_management.db_ids import mongo_ids
from db_management.model import ids

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
import time
from numpy.random import uniform as runi

from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

from func import read_credentials


# BASE_DIR = os.path.abspath(os.path.realpath(__file__))
# BASE_DIR = os.path.join(os.path.dirname(BASE_DIR), '..', '..')
# os.chdir(BASE_DIR)
BASE_DIR = ''

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# create file handler which logs info messages
fh = logging.FileHandler(os.path.join(BASE_DIR, 'logs.txt'), 'w', 'utf-8')
fh.setLevel(logging.DEBUG)

# create console handler with a debug log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# creating a formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-8s: %(message)s')

# setting handler format
ch.setFormatter(formatter)
fh.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(ch)
logger.addHandler(fh)



def extract_integer(a):
    n = [np.int(x) for x in a.split() if x.isdigit()] or [0]
    return n[0]



# def main():
if __name__ == "__main__":

    fname = "df.csv"
    df = pd.read_csv(fname).replace(np.nan, '')

    print('success')
    link = 'https://en.wikipedia.org/wiki/'


    logger.debug('opening browser with scopus advanced search link')

    timeout = 30
    n = 15

    binary = FirefoxBinary('/usr/lib/firefox/firefox')
    driver = webdriver.Firefox(firefox_binary=binary)
    driver.set_page_load_timeout(timeout) # error if timeout has passed

    driver.get(link)

    time.sleep(3)

    cname_name = 'name'
    cname_date = 'establishment_date'
    cname_year_value = 'establishment_year'
    cname_source = 'source_link'
    cname_concern = 'concerns'

    reg_exp_year = r'\d{4}'
    reg_com_year = re.compile(reg_exp_year)


    xpath_est = '//tbody//tr//th[text()="Established"]'
    xpath_founded = '//tbody//tr//th[text()="Founded"]'

    xpath_est_date = '//tbody//tr//th[text()="Established"]/following-sibling::td'
    xpath_est_date_short = './following-sibling::td'

    # driver.find_element_by_xpath('//tbody//tr//th[@value="Established"]')


    # for index, row in df.iloc[:10,:].iterrows():
    for index, row in df.iterrows():

        time.sleep(2+runi(0.5, 1.5))

        if row[cname_date] != '':
            continue

        aff = row[cname_name]
        logger.debug("extracting data for {}".format(aff))

        aff = row[cname_name]

        aff_name_in_link = aff.replace(' ', '_')

        aff_link = link + aff_name_in_link


        try:
            logger.debug('opening link {}'.format(aff_link))
            driver.get(aff_link)
        except:
            logger.warning('problems during opening link {}'.format(aff_link))
            logger.warning('moving to next university'.format(aff_link))
            continue
        else:
            logger.debug('link opened successfully')


        try:
            logger.debug("trying to get establishment by_xpath via 'Established' header")
            try:
                element_est = driver.find_element_by_xpath(xpath_est)
            except:
                logger.debug("'Established' header was not found, moving to 'Founded' header")
                logger.debug("trying to get establishment by_xpath via 'Founded' header")
                element_est = driver.find_element_by_xpath(xpath_founded)


        except:
            logger.warning("coudn't find 'Established' table_header, moving to next university")
            continue
        else:
            logger.debug("'Established' row header was found")


        try:
            logger.debug("extracting establishment date")
            # el = driver.find_element_by_xpath(xpath_est_date)
            el = element_est.find_element_by_xpath(xpath_est_date_short)
        except:
            logger.warning("coudn't extract establishment date, movign to next university")
            continue
        else:
            logger.debug("establishment_date was extracted successfully")
            d = el.text
            logger.debug("{} was established in {}".format(aff, d))

            # year_value = extract_integer(d)
            reg_res = reg_com_year.search(d)

            try:
                logger.debug("extracting year value from string 'date' {}".format(d))
                year_value = int(reg_res.group())
                logger.debug("year value is {}".format(year_value))
            except:
                logger.debug('could not extract year integer from text, setting year_value to null')
                year_value = ''


            logger.debug("updating df at {}".format(aff))
            df.at[index, cname_date ] = d
            df.at[index, cname_year_value ] = year_value
            df.at[index, cname_source] = aff_link


            if year_value != '':
                if year_value > 1950:
                    df.at[index, cname_concern] = 1


    df.to_csv("try2000.csv", index=False)
    # df.to_csv(fname, index=False)

