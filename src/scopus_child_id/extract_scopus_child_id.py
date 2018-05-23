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


BASE_DIR = os.path.abspath(os.path.realpath(__file__))
BASE_DIR = os.path.join(os.path.dirname(BASE_DIR), '..', '..')
os.chdir(BASE_DIR)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create file handler which logs info messages
fh = logging.FileHandler(os.path.join(BASE_DIR, 'logs', 'logs.txt'), 'w', 'utf-8')
fh.setLevel(logging.DEBUG)

# create console handler with a debug log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# creating a formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-8s: %(message)s')

# setting handler format
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)


def extract_integer(a):
    n = [np.int(x) for x in a.split() if x.isdigit()] or [0]
    return n[0]


def open_scival_log_in(driver):

    link = 'https://scival.com/customer/authenticate/loginfull'

    try:

        driver.get(link)

        logger.debug('driver opened succesfully')

    except Exception as e:

        logger.warning('error has occured during opening browser')
        raise


    t = runi(10 + runi(-3, 1))
    logger.debug('opened main link, waiting for {} s'.format(t))
    time.sleep(t)

    return driver


def put_scival_credentials(driver):


    username = 'daniyar.bakir@nu.edu.kz'
    password = read_credentials('password')

    username_field = driver.find_element_by_xpath("//input[@id='username']")
    t = runi(5 + runi(-3, 1))
    time.sleep(t)

    password_field = driver.find_element_by_xpath("//input[@id='password-input-password']")
    t = runi(4 + runi(-3, 1))
    time.sleep(t)

    username_field.send_keys(username)
    password_field.send_keys(password, Keys.RETURN)

    t = runi(10 + runi(-3, 1))
    time.sleep(t)

    return driver


# if __name__ == "__main__":
def get_valid_ids(df, n=None):

    if n is None:
        n = 15

    valid_ids = []
    valid_names = []
    i = 0

    for index, row in df.iterrows():
    # for index, row in df.iloc[0,:].iterrows():

        if i == n:
            break

        if row.id != '':

            if row.scopus_id_downloaded == '':
                valid_ids.append(int(row.id))
                valid_names.append(row.Institution)
                i = i + 1


    return valid_names, valid_ids


def generate_link(a):
    # generate a suitable link for scival

    if not isinstance(a, int):
        a = int(a)

    try:
        if not isinstance(a, int):
            a = int(a)
    except:
        logger.warning('wrong type')
        raise

    return 'https://scival.com/overview/summary?uri=Institution%2F{}'.format(a)


def open_link(driver, link):

    try:

        driver.get(link)
        logger.debug('link opened successfully, {}'.format(link))

    except Exception as e:

        logger.warning('error during opening the link, {}'.format(link))
        raise


    t = runi(10 + runi(-3, 1))
    logger.debug('opened main link, waiting for {} s'.format(t))
    time.sleep(t)

    return driver


def click_more_details(driver):

    try:
        driver.find_element_by_xpath("//a[@id='institutionDetailsLink']").click()
    except:
        driver.find_element_by_xpath("//*[contains(text(), 'More details on this Institution')]").click()

    t = runi(4 + runi(-2, 1))
    logger.debug('opened more details link, waiting for {} s'.format(t))
    time.sleep(t)

    return driver


def extract_table_values(html_table):

    table = html_table.replace('\n', '').replace('\t', '')

    rows = table.split('</tr><tr>')
    header_row = rows[0].split('</thead><tbody>')

    rows[0] = header_row[1]
    header_row = header_row[0]

    # this regex removes everything between adjacent <>
    reg = r'<[^>]*>'

    header_cols = [re.sub(reg, '', x) for x in header_row.split('</th><th>')]

    row_items = []

    for row in rows:
        cur_row = [re.sub(reg, '', x) for x in re.split(reg, row)]
        cur_row = [x for x in cur_row if x != '']

        row_items.append(cur_row)

    df = pd.DataFrame(data=row_items, columns=header_cols).drop('', axis=1)

    return df


def extract_table(driver):

    # retrieve the name and table information from child_id window

    # retrieveInformation
    element = driver.find_element_by_xpath('//*[@id="institutionDetailsWindowContent"]')
    name = element.find_element_by_tag_name('h2').text

    element = driver.find_element_by_xpath('//*[@id="instDetailsContent"]')
    number_text = element.find_element_by_tag_name('h3').text

    html_table = element.find_element_by_xpath("//table[@class='affilTable']").get_attribute('innerHTML')

    table = extract_table_values(html_table)

    child_count = extract_integer(number_text)

    return table, name, child_count


def remove_all_universities(driver):

    logger.debug('removing the universities from the list')
    time.sleep(2 + runi(-0.5, 0.5))

    try:
        driver.find_element_by_xpath("//*[contains(text(), 'Remove all entities from this section')]").click()
        time.sleep(5 + runi(-1,1))
        driver.find_element_by_xpath("//*[contains(text(), 'Remove all entities from this section')]").click()
        time.sleep(5 + runi(-1,1))
        logger.debug('all institutions removed succesfully')
    except:
        logger.warning('error has occured during removing all institutions from the list')
        time.sleep(3 + runi(-0.5, 0.5))


    return driver


def init_db_ids():

    return  mongo_ids()


def append_scopus_ids_to_parent(db_ids, df, parent):

    # append the scopus_ids to a parent in the dbs


    df = df.rename({'Affiliation ID': 'scopus_id', 'Name': 'name'}, axis=1)

    df = df.apply(pd.to_numeric, errors='ignore')

    parent['scival_id'] = int(parent['scival_id'])

    parent_aff = ids(**parent)
    db_ids.insert_affiliation(parent_aff)

    for index, row in df.iterrows():

        db_ids.append_child_aff(parent_aff, 'scival_id', {'scopus_id': row.scopus_id})
        db_ids.insert_affiliation(ids(**row))
        db_ids.append_aff(ids(**row), 'scopus_id', 'parent_id', {'scival_id': parent_aff.scival_id})

    pass


# def main():
if __name__ == "__main__":

    print('success')
    link = 'https://scival.com/customer/authenticate/loginfull'

    fname = 'data/universities_table.csv'
    df = pd.read_csv(fname).replace(np.nan, '')

    aaa = 'https://scival.com/overview/summary?uri=Institution%2F508076'

    logger.debug('initilizing instance of mongo_ids')
    try:
        db_ids = init_db_ids()
        logger.debug('db initialized successfully')
    except:
        logger.warning('error during db initialization')
        raise


    logger.debug('loading df with acknowledgements')

    logger.debug('opening browser with scopus advanced search link')

    timeout = 30
    n = 15

    binary = FirefoxBinary('/usr/lib/firefox/firefox')
    driver = webdriver.Firefox(firefox_binary=binary)
    driver.set_page_load_timeout(timeout) # error if timeout has passed


    logger.debug('open login link')
    driver = open_scival_log_in(driver)


    logger.debug('log in to scival')
    driver = put_scival_credentials(driver)
    logger.debug('loged in successfully')


    logger.debug('getting valid_ids')
    valid_names, valid_ids = get_valid_ids(df, n)
    logger.debug('valid_ids are extracted succesfully')


    logger.debug('generating the links for ids')
    institution_links = [generate_link(x) for x in valid_ids]
    logger.debug('links for ids generated successfully')

    try:

        for i, a in enumerate(institution_links):

            logger.debug('opening link {}'.format(a))
            driver = open_link(driver, a)
            logger.debug('link opened succesfully')


            logger.debug('clicking on "more_details"')
            try:
                driver = click_more_details(driver)
            except:
                logger.warning('could not click on "more_details"')
                raise


            logger.debug('extracting the table')
            try:
                table, extracted_name, child_count = extract_table(driver)
                logger.debug('table extracted succesfully')
            except:
                logger.warning('table could not be extracted')
                raise

            logger.debug('saving the child_ids of {}'.format(valid_ids[i]))
            table.to_excel('data/child_id/{}.xlsx'.format(valid_ids[i]), index=False)


            logger.debug('saving acknoledge to the table')
            df.loc[df.id == valid_ids[i], 'scopus_id_downloaded'] = 1
            df.to_csv(fname, index=False)

            time.sleep(5 + runi(-1, 1))

            parent_aff = {'name': valid_names[i],
                          'scival_id': valid_ids[i]
            }

            append_scopus_ids_to_parent(db_ids, table, parent_aff)


        logger.debug('opening link to remove all universities')
        driver = open_link(driver, a)
        driver = remove_all_universities(driver)

        time.sleep(3 + runi(-1, 1))

    except:
        raise
    finally:

        driver.quit()
        pass


