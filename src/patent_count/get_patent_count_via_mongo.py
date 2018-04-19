import urllib.parse as urlp
import os
import numpy as np
import pandas as pd
from pprint import pprint as pp
import logging
import logging.handlers
import sys


import selenium
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

from db_management.metric_ack import mongo_metric_ack
from db_management.scopus_metrics import mongo_scopus_metrics



BASE_DIR = os.path.abspath(os.path.realpath(__file__))
BASE_DIR = os.path.join(os.path.dirname(BASE_DIR), '..', '..')
os.chdir(BASE_DIR)

FOLNAME_AFF_SEARCH = os.path.join(BASE_DIR, 'data', 'aff_search')
FOLNAME_METRIC_RESPONSE = os.path.join(BASE_DIR, 'data', 'metric_response')

key_aff = 'Institution'
key_acc = 'id_downloaded'
key_id  = 'id'
key_met = 'metrics'

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create file handler which logs info messages
fh = logging.handlers.RotatingFileHandler(os.path.join(BASE_DIR, 'logs', 'logs.txt'), 'a', 1024, 0, 'utf-8')
# fh = logging.handlers.RotatingFileHandler('mptest.log', 'a', 300, 10)
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


def create_query_with_filter(coll, index_field, aff_ids, filter):

    # create a query and filter out some of them

    query = '( ' + ' or '.join(['af-id({})'.format(x) for x in aff_ids]) + ' )'
    query = '{} AND pubyear = {}'.format(query, year)




def extract_integer_(patent_value):

    a = 0

    try:
        logger.debug('extracting integer from patent_value')
        a  = extract_integer(patent_value)
    except:
        logger.debug('error during integer extraction')
        a = 0

    return a


def driver_has_element_by_xpath(driver, xpath):

    try:
        driver.find_element_by_xpath(xpath)
        return True
    except:
        return False


def close_pop_up_window(driver):

    try:
        # driver.find_element_by_xpath("//button/[@id='_pendo-close-guide_']").click()
        driver.find_element_by_xpath("//button[@id='_pendo-close-guide_']").click()
        logger.debug('extra pop_up_window was close')
    except:
        pass


def open_scopus_link(driver):
    '''
    open the advanced search of scopus
    '''
    # binary = FirefoxBinary('/usr/lib/firefox/firefox')
    # driver = webdriver.Firefox(firefox_binary=binary)
    driver.implicitly_wait(2) # seconds

    # l = 'https://www.nobelprize.org/nobel_prizes/physics/laureates/index.html'
    main_scopus2 = 'https://www.scopus.com/sources?zone=&origin=NO%20ORIGIN%20DEFINED'
    main_search = 'https://www.scopus.com/search/form.uri?zone=TopNavBar&origin=sbrowse&display=basic'
    adv_search = 'https://www.scopus.com/search/form.uri?display=advanced&clear=t&origin=searchbasic&txGid=fc476bc6f6c3a112a577edd9f6f26e14'

    # to get to advanced search, we need to go through several links
    driver.get(main_scopus2)
    t = runi(10 + runi(-3, 1))
    logger.debug('opened main link, waiting for {} s'.format(t))
    time.sleep(t)
    # driver.implicitly_wait(10) # seconds

    close_pop_up_window(driver)

    driver.get(main_search)
    t = runi(10 + runi(-3, 1))
    logger.debug('opened search link, waiting for {} s'.format(t))
    time.sleep(t)
    # driver.implicitly_wait(10) # seconds


    close_pop_up_window(driver)

    driver.get(adv_search)
    t = runi(10 + runi(-3, 1))
    logger.debug('opened advanced search link, waiting for {} s'.format(t))
    time.sleep(t)
    # driver.implicitly_wait(10) # seconds

    close_pop_up_window(driver)

    return driver


def extract_integer(a):
    n = [np.int(x) for x in a.split() if x.isdigit()] or [0]
    return n[0]



def get_patent_count(driver, query):

    # click to activate the textField
    # sometimes we need to click 'contentEditLabel', sometimes 'searchfield'
    logger.debug('clicking on search input field')
    try:
        logger.debug('clicking on contentEditLabel')
        driver.find_element_by_id('contentEditLabel').click()
    except Exception as e:
        logger.debug('clicking on searchfield')
        driver.find_element_by_id('searchfield').click()

    # fill the textfield and send the request
    element = driver.find_element_by_id('searchfield')

    logger.debug('clearing search field')
    element.clear()
    # time.wait(runi(0, 0.2))
    logger.debug('entering query into search field')
    element.send_keys(query, Keys.RETURN)

    t = 4 + runi(0, 1)
    time.sleep(t)


    logger.debug('waiting for page to be downloaded')
    # get amount of patents
    el = wait(driver, 60).until(EC.presence_of_element_located((By.ID, "searchResFormId")))

    current_url = driver.current_url
    driver.get(current_url)

    t = 10 + runi(0, 1)
    time.sleep(t)

    # no documents found
    if driver_has_element_by_xpath(driver, '//div[@class="alert alert-danger"]/a[@class="close"]'):
        logger.debug('no document found for here')
        a = 0
        return a
    else:

        # if documents found
        a = 0

        try:

            to_wait = -5 

            while to_wait < 0:

                logger.debug('retrieving patent_count')
                patent_hidden_element = driver.find_element_by_id('hubLinksContainer')
                if patent_hidden_element.get_attribute('class') == 'hidden':
                    logger.debug('no patent elements were found')
                    patent_value = 0
                    to_wait = 5
                elif driver.find_element_by_id('patentLink').is_displayed():
                    logger.debug('getting #patentLink once more')
                    patent_element = driver.find_element_by_id('patentLink')
                    patent_value = patent_element.text
                    time.sleep(1)
                    to_wait = to_wait + 1


        except:
            driver.find_element_by_xpath("//button[@title='Edit search query']").click()
            assert 'something happened'

        else:

            logger.debug('patent_value is {}'.format(patent_value))

            a = extract_integer_(patent_value)

            logger.debug('extracted integer is {}'.format(a))

            t = 2 + runi(0, 1)
            time.sleep(t)

            try:
                logger.debug('clicking on editAuthSearch')
                driver.find_element_by_id('editAuthSearch').click()
            except:
                logger.debug('error during clicking on editAuthSearch')
                logger.debug('instead find "Edit search query" field by force')
                driver.find_element_by_xpath("//button[@title='Edit search query']").click()

            t = 2 + runi(0, 1)
            time.sleep(t)

            return a


def main(n, year, metricType, ack_params, metrics_params):

    adv_search_link = 'https://www.scopus.com/search/form.uri?display=advanced&clear=t&origin=searchbasic&txGid=fc476bc6f6c3a112a577edd9f6f26e14'

    logger.debug('downloaded results table')


    # db_name_ack = 'acknowledgements'
    # coll_name_ack = 'acks_by_scival_Apr19'

    # db_name_metrics = 'scopus_metrics'
    # coll_name_metrics = 'metrics_by_scival'


    db_name_ack = ack_params['db_name']
    coll_name_ack = ack_params['coll_name']

    db_name_metrics = metrics_params['db_name']
    coll_name_metrics = metrics_params['coll_name']

    parent_field = 'scival_id'
    child_field = 'scopus_id'
    child_id_field = 'child_id'

    coll_ack = mongo_metric_ack(db_name=db_name_ack, coll_name=coll_name_ack)
    coll_metrics = mongo_scopus_metrics(db_name=db_name_metrics, coll_name=coll_name_metrics)


    # valid_ids = coll_ack.find_valid_parent_ids(metricType, str(year), n)
    valid_dicts = coll_ack.find_valid_parent_ids(metricType, str(year), n)

    logger.debug('opening browser with scopus advanced search link')

    timeout = 60

    fp = webdriver.FirefoxProfile()
    fp.set_preference("http.response.timeout", timeout)
    fp.set_preference("dom.max_script_run_time", timeout)

    binary = FirefoxBinary('/usr/lib/firefox/firefox')
    driver = webdriver.Firefox(firefox_binary=binary, firefox_profile=fp)


    try:
        # driver = open_scopus_link(driver)
        driver.get(adv_search_link)
        time.sleep(5)

        logger.debug('doing search')
    except:
        logger.warning('error has occured during opening browser')

    else:


        for valid_dict in valid_dicts:


            logger.debug('opening advanced search link')
            driver.get(adv_search_link)
            time.sleep(5+runi(-1, 1))

            close_pop_up_window(driver)

            patent_count = 0

            parent_id = valid_dict[parent_field]
            aff_id = valid_dict[child_id_field]

            logger.debug('creating query for search')
            # query = 'af-id({}) AND pubyear = {}'.format(aff_id, year)
            query = '( ' + ' or '.join(['af-id({})'.format(x) for x in aff_id]) + ' )'
            query = '{} AND pubyear = {}'.format(query, year)
            logger.debug('query is {}'.format(query))


            q = {parent_field: parent_id,
                 'metricType': metricType,
                 'year': year}

            metric_response = q.copy()
            ack_response = q.copy()


            try:
                logger.debug('getting patent_count for {}'.format(aff_id))
                patent_count = get_patent_count(driver, query)

            except TimeoutException as e:
                logger.warning('timeout error')
                print(e)
                break

            except Exception as e:
                logger.warning('error has occured')
                logger.warning(e)

                ack_response['ack'] = -1
                coll_ack.update_item_by_year(parent_field, **ack_response)

                break

            else:

                # saving response and ack
                metric_response['value'] = patent_count
                ack_response['ack'] = 1

                logger.debug('number of patents has been retrieved succesfully')
                logger.debug('number of patents for {} is {}'.format(parent_id, patent_count))

                logger.debug("updating metrics and ack dbs")
                coll_metrics.update_item_by_year(parent_field, **metric_response)
                coll_ack.update_item_by_year(parent_field, **ack_response)
                logger.debug("updating metrics and ack dbs finished")

            print(ack_response)


    finally:
        driver.quit()
        pass

    return driver


