import numpy as np
import pandas as pd
import pymongo
import os
import errno
import logging
from my_scival import InstitutionSearch, MetricSearch

from urllib import parse, request
from urllib.error import HTTPError
from func import get_InstitutionSearch, get_aff_id
from func import *

import pickle as pk
from pprint import pprint as pp

BASE_DIR = os.path.abspath(os.path.realpath(__file__))
BASE_DIR = os.path.join(os.path.dirname(BASE_DIR), '..')
os.chdir(BASE_DIR)

FOLNAME_AFF_SEARCH = os.path.join(BASE_DIR, 'data', 'aff_search')
FOLNAME_METRIC_RESPONSE = os.path.join(BASE_DIR, 'data', 'metric_response')

key_aff = 'Institution'
key_acc = 'id_downloaded'
key_id  = 'id'
key_met = 'metrics'

logger = logging.getLogger(__name__ + "InstitutionSearch")
logger.setLevel(logging.DEBUG)

# create file handler which logs info messages
fh = logging.FileHandler(os.path.join('logs', 'logs.txt'), 'w', 'utf-8')
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


def pd_write_data(df, d, aux_key=None, aux_val=None):
    """
    write data from dict d to pandas.dataframe df
    data will be written to columns of df by using keys
    """

    for key, value in d.items():
        df.at[d['name'], key] = value

        if aux_key is not None and aux_val is not None:
            df.at[d['name'], aux_key] = aux_val

    return df



# trying to retrieve the id of the university
if __name__=="__main__":

    """
    get the InstitutionSearch output for every affiliation that was not already downloaded
    """

    MY_API_KEY = "e53785aedfc1c54942ba237f8ec0f891"
    # MY_API_KEY = "7f59af901d2d86f78a1fd60c1bf9426a"

    logger.debug('loading university names')
    fname_aff_names = os.path.join(BASE_DIR, 'data', "universities_table.csv")

    df = pd.read_csv(fname_aff_names).set_index(key_aff)

    a = df[key_acc].replace(np.nan,0) == 0     # which universities are not downloaded
    all_aff_names = df.index[a].tolist()

    n = 20
    responses = []
    jsons = []

    dff = df.copy()
    dff = dff.replace(0, '')

    logger.debug('starting to load institution_id')
    for i in range(n):

        aff_name = all_aff_names[i]
        logger.debug('aff_name is {}'.format(aff_name))

        res = get_InstitutionSearch(aff_name, MY_API_KEY)
        dict_res, json_res = get_aff_id(res)
        responses.append(dict_res)
        jsons.append(json_res)

        fname_save_responses = 'responses_{}.pickle'.format(aff_name)
        fname_save_json = 'json{}.pickle'.format(aff_name)

        fname_save_responses = os.path.join(FOLNAME_AFF_SEARCH, fname_save_responses)
        fname_save_json = os.path.join(FOLNAME_AFF_SEARCH, fname_save_json)

        logger.debug('saving responses and json response to {} and {} respectively'.format(fname_save_responses, fname_save_json))
        pk.dump(dict_res, open(fname_save_responses, 'wb'))
        pk.dump(json_res, open(fname_save_json, 'wb'))

        for x in dict_res:
            dff = pd_write_data(dff, x, key_acc, 1)

        logger.debug('updating csv file {}'.format(fname_aff_names))
        dff.to_csv(fname_aff_names)
