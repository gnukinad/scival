import urllib.parse as urlp
import os
import numpy as np
import pandas as pd
from pprint import pprint as pp
import logging

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


BASE_DIR = os.path.abspath(os.path.realpath(__file__))
BASE_DIR = os.path.join(os.path.dirname(BASE_DIR), '..')
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


def get_valid_ids(df, key, n, valid_values=None, bad_values=None):
    '''this function gets the valid indices'''

    valid_values = [np.nan] or valid_values
    bad_values = [-1] or bad_values

    # a = df.loc[:, key]

    valid_inds = []

    inds = df.index.tolist()

    i = 0

    while(True):

        if np.isnan(df.loc[inds[i], key]):
            valid_inds.append(inds[i])
            i = i+1

        if i == n:
            break

    return valid_inds




class my_df(pd.DataFrame):

    def __init__(self):

        super().__init__(self)
        self.i = 0

    def my_iterrows(self):
        a = self.iterrows()
        self.i = self.i + 1
        return a

    def get_valid_id(self, key=None):

        q = 0

        while(True):
            
            a = next(self.iterrows())

            print(a)
            q = q + 1

            if q % 5 == 0:
                break
            

def open_scopus_link():
    '''
    open the advanced search of scopus
    '''
    binary = FirefoxBinary('/usr/lib/firefox/firefox')
    driver = webdriver.Firefox(firefox_binary=binary)
    # driver.
    driver.implicitly_wait(2) # seconds

    # l = 'https://www.nobelprize.org/nobel_prizes/physics/laureates/index.html'
    main_scopus2 = 'https://www.scopus.com/sources?zone=&origin=NO%20ORIGIN%20DEFINED'
    main_search = 'https://www.scopus.com/search/form.uri?zone=TopNavBar&origin=sbrowse&display=basic'
    adv_search = 'https://www.scopus.com/search/form.uri?display=advanced&clear=t&origin=searchbasic&txGid=fc476bc6f6c3a112a577edd9f6f26e14'

    # to get to advanced search, we need to go through several links
    driver.get(main_scopus2)
    t = runi(10 + runi(-3, 1))
    print('waiting for {} s'.format(t))
    time.sleep(t)
    # driver.implicitly_wait(10) # seconds

    driver.get(main_search)
    t = runi(10 + runi(-3, 1))
    print('waiting for {} s'.format(t))
    time.sleep(t)
    # driver.implicitly_wait(10) # seconds

    driver.get(adv_search)
    t = runi(10 + runi(-3, 1))
    print('waiting for {} s'.format(t))
    time.sleep(t)
    # driver.implicitly_wait(10) # seconds

    return driver


def extract_integer(a):
    n = [np.int(x) for x in a.split() if x.isdigit()] or 0
    return n[0]


def unparse(l):
    main_link, query = l.split('?')

    a = query.split('&')

    b = [x.split('=') for x in a]
    params = dict(zip([x[0] for x in b], [x[1] for x in b]))

    q = params['s'].split('AND')
    qparams = [urlp.unquote_plus(x).strip() for x in q]

    params['q'] = qparams
    params['main_link'] = main_link

    return params


def get_patent_count(driver, query):

    # click to activate the textField
    print('clicking on contentEditLabel')
    try:
        driver.find_element_by_id('contentEditLabel').click()
    except Exception as e:
        driver.find_element_by_id('searchfield').click()


    # fill the textfield and send the request
    element = driver.find_element_by_id('searchfield')

    print('clearing query field')
    element.clear()
    # time.wait(runi(0, 0.2))
    print('entering value')
    element.send_keys(query, Keys.RETURN)


    print('waiting for page download')
    # get amount of patents
    text_of_element_on_new_page = wait(driver, 15).until(EC.presence_of_element_located((By.ID, "patentLink"))).text

    t = 4 + runi(0, 1)
    time.sleep(t)

    print('retrieving patent count')
    patent_element = driver.find_element_by_id('patentLink')
    patent_value = patent_element.text

    print(patent_element)
    print(patent_value)

    try:
        a  = extract_integer(patent_value)
    except:
        a = 0

    print(a)

    t = 2 + runi(0, 1)
    time.sleep(t)

    driver.find_element_by_id('editAuthSearch').click()

    t = 2 + runi(0, 1)
    time.sleep(t)

    return a


if __name__=="__main__":

    slink = 'https://www.scopus.com/results/results.uri?sort=plf-f&src=s&sid=9cf773e35f9165af3488aaee575fd58a&sot=a&sdt=a&sl=49&s=affil%28%22University+of+Toronto%22%29+AND+pubyear+%3d+2016&origin=searchadvanced&editSaveSearch=&txGid=7bfd7d6ce111fb51284b6d3d7d6149ee'

    plink = 'https://www.scopus.com/results/results.uri?src=p&sort=plf-f&sid=9cf773e35f9165af3488aaee575fd58a&sot=a&sdt=a&sl=49&s=affil%28%22University+of+Toronto%22%29+AND+pubyear+%3d+2016&cl=t&offset=1&ss=plf-f&ws=r-f&ps=r-f&cs=r-f&origin=resultslist&zone=queryBar'

    simple = unparse(slink)
    patent = unparse(plink)

    n = 3

    res_ind_name = 'name'
    res_col_name = 'patent_count'
    res = pd.read_csv('data/patent_count_2016.csv').set_index(res_ind_name)
    

    logger.debug('loading df with acknowledgements')
    df = pd.read_csv('data/universities_table.csv')
    key = 'patent_downloaded_2016'
    valid_aff_names = get_valid_ids(df.set_index('Institution'), key, n)


    driver = open_scopus_link()
    year = 2015

    curd = {res_ind_name: [],
            res_col_name: []}

    for i in range(n):
        aff_name = valid_aff_names[i]
        patent_count = 0

        query = 'affil("{}") AND pubyear = {}'.format(aff_name, year)

        try:
            logger.debug('getting patent_count for {}'.format(aff_name))
            patent_count = get_patent_count(driver, query)
            
            logger.debug('number of patents for {} is {}'.format(aff_name, patent_count))

            df.at[aff_name, key] = 1

        except Exception as e:
            logger.warning('error has occured')
            logger.warning('error is ', e)

            df.at[aff_name, key] = -1

        curd[res_ind_name].append(aff_name)
        curd[res_col_name].append(patent_count)

    cur_df = pd.DataFrame(curd)
    res = pd.concat([res.reset_index(), cur_df.reset_index()], ignore_index=True)


