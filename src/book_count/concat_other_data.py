import pandas as pd
import numpy as np
import os
import glob
import re

import logging
from pprint import pprint as pp
import json

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
fh = logging.FileHandler(os.path.join(BASE_DIR, 'logs.txt'), 'w', 'utf-8')
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


def get_patent_count(fname_book, fname_res):

    # fname_book = '../data/patent_count.csv'
    # fname_res = '../data/long_patent_count.csv'
    df_book = pd.read_csv(fname_book)

    patent_cols = ['patent_count_2012', 'patent_count_2013', 'patent_count_2014', 'patent_count_2015', 'patent_count_2016']
    convert_dict = {'patent_count_2012': 2012,
                    'patent_count_2013': 2013,
                    'patent_count_2014': 2014,
                    'patent_count_2015': 2015,
                    'patent_count_2016': 2016}

    years = [2012, 2013, 2014, 2015, 2016]
    pname = 'patent_count'

    df = pd.DataFrame(columns=['name', 'year', 'metricType', 'valueByYear'])

    se = re.compile(pname)

    for index, row in df_book.iterrows():

        for sindex, svalue in row.iteritems():

            if se.search(sindex):

                if not np.isnan(svalue):

                    df.at[index, 'name'] = row['name']
                    df.at[index, 'metricType'] = 'PatentCount'
                    splitted = sindex.split('_')
                    year = splitted[-1]
                    df.at[index, 'valueByYear'] = svalue
                    df.at[index, 'year'] = year

    df.reset_index().to_csv(fname_res, index=False)


def get_book_count(res):

    nbooks = res['opensearch:totalResults']
    search_query = res['opensearch:Query']['@searchTerms']

    splitted = search_query.split('AND')

    se = re.compile('affil', re.IGNORECASE)

    q = [x for x in splitted if se.search(x)][0].strip()
    aff_name = q[q.find("(")+1:q.find(")")].strip('"')

    return {'name': aff_name,
            'book_count': nbooks}


def append_book_count(res_name, year):

    # res_name = '../data/book_count.csv'

    df = pd.DataFrame()

    res = dict()

    # year = 2014

    dname_book_count = 'data/aff_book_' + str(year)
    fnames = os.listdir(dname_book_count)

    for x in fnames:
        # print('reading ', x)
        with open('data/aff_book_' + str(year) + '/' + x) as f:
            txt = f.readlines()

        jres = json.loads(txt[0])

        a = get_book_count(jres['search-results'])

        q = {'name': a['name'],
             'metricType': 'BookCount',
             'year': year,
             'valueByYear': a['book_count']}

        df = df.append(q, ignore_index=True)

    try:
        df_old = pd.read_csv(res_name)
    except:
        df_old = pd.DataFrame(columns=['name', 'metricType', 'year', 'valueByYear'])

    df = pd.concat([df, df_old])

    df.to_csv(res_name, index=False)
    return df


if __name__ == "__main__":

    fname_long_book = 'data/long_book_count2.csv'

    years = [2014, 2015, 2016]
    # years = [2012]
    for x in years:
        df = append_book_count(fname_long_book, x)


