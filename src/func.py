import numpy as np
import pandas as pd
import pymongo
import os
import errno
import logging
from my_scival import InstitutionSearch, MetricSearch

from urllib import parse, request
from urllib.error import HTTPError

from pprint import pprint

BASE_DIR = os.path.abspath(os.path.realpath(os.path.dirname(__file__)))
BASE_DIR = os.path.join(BASE_DIR, "..")

logger = logging.getLogger(__name__ + "InstitutionSearch")
logger.setLevel(logging.DEBUG)

fname_log = os.path.join(BASE_DIR, 'logs', 'logs.txt')

# create file handler which logs info messages
fh = logging.FileHandler(fname_log, 'w', 'utf-8')
fh.setLevel(logging.INFO)

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


def get_InstitutionSearch(aff_name=None, api_key=None):

    if api_key is None:
        raise NameError("api_key is not found")

    if aff_name is None:
        raise NameError("affiliation name is not found")

    a = InstitutionSearch(query_type="name", universityName=aff_name, apiKey=api_key, fname_log=fname_log)
    a.get_jres()
    return a


def get_InstitutionMetric(aff_id=None, api_key=None):

    if api_key is None:
        raise NameError("api_key is not found")

    if aff_id is None:
        raise NameError("affiliation id is not found")

    a = MetricSearch(aff_id=aff_id, apiKey=api_key)
    a.get_jres()
    return a


def get_aff_id(jres):

    """
    get the id and json response from InsitutionSearch object

    input:
    InstitutionSearch object

    output:
    id - university id
    jres - json response
    """

    if isinstance(jres['results'], list):
        logger.debug('retrieving all keys from a lsit of dicts')
        l = []
        for x in jres['results']:
            a = dict()
            a['link'] = x['link']['@href']
            a['type'] = x['link']['@type']
            a['id'] = x['id']
            a['name'] = x['name']
            a['uri'] = x['uri']
            a['country'] = x['country']
            a['countryCode'] = x['countryCode']
            a['region'] = x['region']
            a['sector'] = x['sector']
            l.append(a)
    elif isinstance(jres['results'], dict):
        logger.debug('retrieving all keys from a dict')
        l = dict()
        l['link'] = x['link']['@href']
        l['type'] = x['link']['@type']
        l['id'] = x['id']
        l['name'] = x['name']
        l['uri'] = x['uri']
        l['country'] = x['country']
        l['countryCode'] = x['countryCode']
        l['region'] = x['region']
        l['sector'] = x['sector']

    return l, jres


def pd_dict_list(a):
    """
    concat a list of dicts
    
    inputs:
    a - list of dicts

    output:
    df - data frame
    """

    df = pd.concat([pd.DataFrame.from_dict(x) for x in a])
    df.index.name = 'year'
    return df



def get_aff_metrics(res):
    """
    get all metrics in a single pandas data frame

    input:
    res - a dictionary with the metrics defined by Scival

    output:
    df - dataframe with all metrics
    """

    df_list = []
    for x in res:

        mtyp = x['metric']['metricType']
        # mval = x['metric']['values']
        mval = x['metric']

        cols_nested = ['CollaborationImpact', 'OutputsInTopCitationPercentiles', 'Collaboration', 'PublicationsInTopJournalPercentiles']
        cols_normal = ['FieldWeightedCitationImpact', 'CitedPublications', 'CitationsPerPublication', 'ScholarlyOutput', 'CitationCount']
        

        if mtyp in cols_nested:
            tmp = pd_dict_list(mval['values'])
            tmp = tmp.assign(metricType=mtyp)
        elif mtyp in cols_normal:
            tmp = pd.DataFrame.from_dict(mval)
            tmp.index.name = 'year'

            if mtyp == 'PublicationsInTopJournalPercentiles':
                tmp.assign(impactType=x['metric']['impactType'])

        tmp = tmp.assign(link=x['links'][0]['@href'])
        tmp = tmp.assign(type=x['links'][0]['@type'])
        tmp = tmp.assign(id  =x['institution']['id'])
        tmp = tmp.assign(name=x['institution']['name'])
        tmp = tmp.assign(uri =x['institution']['uri'])

        df_list.append(tmp)

    df = pd.concat(df_list)
    
    return df


def find_dict_cols(df):
    """
    check the column value of data frame columns
    """

    cols = df.columns.tolist()

    a = [x for x in cols if isinstance((df.loc[:, x][0]), dict)]
    return a
