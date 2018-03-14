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
    print(inds[:10])

    i = 0
    j = 0

    while(True):

        if np.isnan(df.loc[inds[j], key]):
            valid_inds.append(inds[j])
            i = i+1

        if i == n:
            break

        if j > df.shape[0]:
            break

        if i % 10 == 0 and i != 0:
            logger.debug('getting valid inds, i is {}'.format(i))

        j = j + 1

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

    driver.get(main_search)
    t = runi(10 + runi(-3, 1))
    logger.debug('opened search link, waiting for {} s'.format(t))
    time.sleep(t)
    # driver.implicitly_wait(10) # seconds

    driver.get(adv_search)
    t = runi(10 + runi(-3, 1))
    logger.debug('opened advanced search link, waiting for {} s'.format(t))
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
    logger.debug('clicking on contentEditLabel')
    try:
        driver.find_element_by_id('contentEditLabel').click()
    except Exception as e:
        driver.find_element_by_id('searchfield').click()



    # fill the textfield and send the request
    element = driver.find_element_by_id('searchfield')

    logger.debug('clearing search field')
    element.clear()
    # time.wait(runi(0, 0.2))
    logger.debug('entering query into search field')
    element.send_keys(query, Keys.RETURN)


    logger.debug('waiting for page to be downloaded')
    # get amount of patents
    el = wait(driver, 5).until(EC.presence_of_element_located((By.ID, "searchResFormId")))

    current_url = driver.current_url
    driver.get(current_url)

    t = 4 + runi(0, 1)
    time.sleep(t)

    try:
        logger.debug('retrieving patent.count')
        patent_hidden_element = driver.find_element_by_id('hubLinksContainer')
        if patent_hidden_element.get_attribute('class') == 'hidden':
            patent_value = 0
        else:
            patent_element = driver.find_element_by_id('patentLink')
            patent_value = patent_element.text
    
            try:
                a  = extract_integer(patent_value)
            except:
                a = 0
    
        t = 2 + runi(0, 1)
        time.sleep(t)
    
        driver.find_element_by_id('editAuthSearch').click()
    
        t = 2 + runi(0, 1)
        time.sleep(t)
    
        return a
    except:
        driver.find_element_by_xpath("//button[@title='Edit search query']").click()
        assert 'something happened'


'''
if __name__=="__main__":

    n = 3

    year = 2013

    fname_df = os.path.join('data', 'universities_table.csv')
    fname_res = os.path.join('data', 'patent_count.csv')
'''


def main(n, year, fname_df, fname_res):

    slink = 'https://www.scopus.com/results/results.uri?sort=plf-f&src=s&sid=9cf773e35f9165af3488aaee575fd58a&sot=a&sdt=a&sl=49&s=affil%28%22University+of+Toronto%22%29+AND+pubyear+%3d+2016&origin=searchadvanced&editSaveSearch=&txGid=7bfd7d6ce111fb51284b6d3d7d6149ee'

    plink = 'https://www.scopus.com/results/results.uri?src=p&sort=plf-f&sid=9cf773e35f9165af3488aaee575fd58a&sot=a&sdt=a&sl=49&s=affil%28%22University+of+Toronto%22%29+AND+pubyear+%3d+2016&cl=t&offset=1&ss=plf-f&ws=r-f&ps=r-f&cs=r-f&origin=resultslist&zone=queryBar'

    simple = unparse(slink)
    patent = unparse(plink)

    # n = 15
    # year = 2015

    logger.debug('downloaded results table')
    # fname_res = os.path.join('data', 'patent_count.csv')

    res_ind_name = 'name'
    res_col_name = 'patent_count_{}'.format(year)
    res = pd.read_csv(fname_res).set_index(res_ind_name)
    

    # fname_df = os.path.join('data', 'universities_table.csv')
    # fname_res = os.path.join('data', 'patent_count.csv')
    logger.debug('loading df with acknowledgements')
    df = pd.read_csv(fname_df).set_index('Institution')


    key = 'patent_downloaded_{}'.format(year)
    logger.debug('loading affiliations that needs to be processed, key is {}, n is {}'.format(key, n))
    valid_aff_names = get_valid_ids(df, key, n)
    # valid_aff_names = get_valid_ids(df.set_index('Institution'), key, n)

    logger.debug('opening browser with scopus advanced search link')

    timeout = 30

    binary = FirefoxBinary('/usr/lib/firefox/firefox')
    driver = webdriver.Firefox(firefox_binary=binary)
    driver.set_page_load_timeout(timeout) # error if timeout has passed
    
    try:
        driver = open_scopus_link(driver)

        curd = {res_ind_name: [],
            res_col_name: []}

        logger.debug('doing search')
    except:
        logger.warning('error has occured during opening browser')
    else:

        # for i in range(n):
        for x in valid_aff_names:
            aff_name = x
            # aff_name = valid_aff_names[i]
            patent_count = 0
    
            logger.debug('creating query for search')
            query = 'affil("{}") AND pubyear = {}'.format(aff_name, year)
            logger.debug('query is {}'.format(query))
    
            try:
                logger.debug('getting patent_count for {}'.format(aff_name))
                patent_count = get_patent_count(driver, query)
                
                logger.debug('number of patents for {} is {}'.format(aff_name, patent_count))
                logger.debug('number of patents for {} is {}'.format(aff_name, patent_count))
                logger.debug('updating acknoledgment df at [{}, {}]'.format(aff_name, key))
                df.at[aff_name, key] = 1
    
                logger.debug('updating the resultant dictionary')
                curd[res_ind_name].append(aff_name)
                curd[res_col_name].append(patent_count)
    
            except Exception as e:
                logger.warning('error has occured')
    
                df.at[aff_name, key] = -1
    
    
        logger.debug('creating the resultant DataFrame')
        cur_df = pd.DataFrame(curd).set_index('name')
        res = pd.concat([res.reset_index(), cur_df.reset_index()], ignore_index=True).set_index('name')
    
        logger.debug('saving the results')
        res.to_csv(fname_res)
        df.to_csv(fname_df)
    
    finally:
        driver.quit()



if __name__ == "__main__":

    # n = 15
    # year = 2015
    # fname_df = 
    # fname_res = 

    n = 3
    year = 2014

    fname_df = os.path.join('data', 'universities_table.csv')
    fname_res = os.path.join('data', 'patent_count.csv')

    for i in range(1):
        main(n=n, year=year, fname_df=fname_df, fname_res=fname_res)
        time.sleep(5)

