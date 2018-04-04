import pandas as pd
import numpy as np
import os
import glob
import re

import logging
from pprint import pprint as pp
import json


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
                    df.at[index, 'metricType'] = 'patentCount'
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

    dname_book_count = '../data/aff_book_' + str(year)
    fnames = os.listdir(dname_book_count)

    for x in fnames:
        # print('reading ', x)
        with open('../data/aff_book_' + str(year) + '/' + x) as f:
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

    """

    years = [2012, 2013, 2014, 2015, 2016]
    # years = [2012]
    for x in years:
        df = append_book_count(fname_long_book, x)


    fname_book = '../data/patent_count.csv'
    # get_patent_count(fname_book, fname_long_patent)
    """

    dname_metric = os.path.join("..", "data", "metric_response")

    drop_cols = ['uri', 'type', 'link']
    excel_fname = "long_table.xlsx" # soon be renamed into all_metrics.xlsx

    all_fnames = glob.glob(os.path.join(dname_metric, "*.csv"))


    df_list = [pd.DataFrame(pd.read_csv(x)) for x in all_fnames]
    df_metric = pd.concat(df_list)

    fname_long_book = '../data/long_book_count.csv'
    fname_long_patent  = '../data/long_patent_count.csv'

    df_patent = pd.read_csv(fname_long_book)
    df_book   = pd.read_csv(fname_long_patent)

    df = pd.concat([df_metric, df_patent, df_book], ignore_index=True)

<<<<<<< HEAD
<<<<<<< HEAD
    df.to_csv('../data/long_all_metrics.csv', index=False)
=======
    df.to_csv('../data/long_all_metrics3.csv', index=False)
>>>>>>> 0f1daa2... changed file datanames and some field names
