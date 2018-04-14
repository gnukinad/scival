import numpy as np
import pandas as pd
import pymongo
import os
import errno
import logging

from urllib import parse, request
# from urllib.error import HTTPError
from func import get_InstitutionSearch, get_aff_id, read_credentials
from my_scival import InstitutionSearch, MetricSearch

import pickle as pk
from pprint import pprint as pp
import urllib.error


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


class my_df_id():

    def __init__(self, df):

        self.i = 0
        self.df = df.replace(np.nan, 0).copy()
        self.n_inds = len(self.df.index.tolist())
        self.inds = self.df.index.tolist()


    def next_row(self):

        if self.i < self.n_inds:

            ind = self.inds[self.i]
            row = df.loc[ind, :]

            return row

        else:
            return -1


    def next_aff_name(self):


        while(True):

            row = self.next_row()

            if isinstance(row, int) is False:

                if row['is_downloaded'] == 0:
                    name = row.name
                    break

            elif isinstance(row, int):

                if row == -1:
                    name = '-1'
                    raise("sorry, the end of DataFrame was reached")
                    break

        return name




def pd_write_data(df, d, aux_key=None, aux_val=None):
    """
    write data from dict d to pandas.dataframe df
    data will be written to columns of df by using keys
    """

    # if the university not in the table
    if not (d['name'] in df.index.tolist()):
        index_name = df.index.name
        df = df.reset_index().append({index_name: d['name']}, ignore_index=True).set_index(index_name)

    for key, value in d.items():

        df.at[d['name'], key] = value

        if aux_key is not None and aux_val is not None:
            df.at[d['name'], aux_key] = aux_val

    return df


if __name__=="__main__":

    """
    get the InstitutionSearch output for every affiliation that was not already downloaded
    """

    MY_API_KEY = read_credentials("MY_API_KEY")
    # MY_API_KEY = "e53785aedfc1c54942ba237f8ec0f891"
    # MY_API_KEY = "7f59af901d2d86f78a1fd60c1bf9426a"

    logger.debug('loading university names')
    fname_aff_names = os.path.join(BASE_DIR, 'data', "universities_table.csv")

    df = pd.read_csv(fname_aff_names).set_index(key_aff)

    a = df[key_acc].replace(np.nan,0) == 0     # which universities are not downloaded
    all_aff_names = df.index[a].tolist()

    n = 15
    responses = []
    jsons = []

    dff = df.copy()
    dff = dff.replace(0, '')

    logger.debug('finalizing affiliation ids to get')
    aff_name = []
    for i in range(n):

        aff_name.append(all_aff_names[i])
        logger.debug('aff_name is {}'.format(aff_name[i]))

        # res = get_InstitutionSearch(aff_name, MY_API_KEY)

    try:
        logger.debug('retrieving aff ids')
        res = InstitutionSearch(query_type="name", universityName=aff_name, apiKey=MY_API_KEY, logger=logger).get_jres()

        dict_res, json_res = get_aff_id(res.jres)
        logger.debug('aff ids retrieval is successful')

        responses.append(dict_res)
        jsons.append(json_res)

        fname_save_responses = 'responses_{}_{}.pickle'.format(aff_name[0], n)
        fname_save_responses = os.path.join(FOLNAME_AFF_SEARCH, fname_save_responses)
        pk.dump(dict_res, open(fname_save_responses, 'wb'))

        """
        fname_save_json = 'json{}.pickle'.format(aff_name)

        fname_save_responses = os.path.join(FOLNAME_AFF_SEARCH, fname_save_responses)
        fname_save_json = os.path.join(FOLNAME_AFF_SEARCH, fname_save_json)

        logger.debug('saving responses and json response to {} and {} respectively'.format(fname_save_responses, fname_save_json))
        pk.dump(dict_res, open(fname_save_responses, 'wb'))
        pk.dump(json_res, open(fname_save_json, 'wb'))
        """

        # pp(res.jres)

        for x in aff_name:
            dff.at[x, key_acc] = 1

        for x in dict_res:
            logger.debug('updating acknowledgement in the table for affiliation {}'.format(x))
            dff = pd_write_data(dff, x, key_acc, 1)

    except Exception as e:

        if res.http_error in [401, 429]:
            logger.debug("error retrieved, error is {}".format(res.http_error))
        else:
            logger.debug("error retrieved, error is {}".format(res.http_error))
            dff.at[aff_name, key_acc] = -1
        
    logger.debug('updating csv file {}'.format(fname_aff_names))
    dff.to_csv(fname_aff_names)




    """
        if res.jres is not None:
            dict_res, json_res = get_aff_id(res.jres)
            responses.append(dict_res)
            jsons.append(json_res)

            fname_save_responses = 'responses_{}.pickle'.format(aff_name)
            fname_save_json = 'json{}.pickle'.format(aff_name)

            fname_save_responses = os.path.join(FOLNAME_AFF_SEARCH, fname_save_responses)
            fname_save_json = os.path.join(FOLNAME_AFF_SEARCH, fname_save_json)

            logger.debug('saving responses and json response to {} and {} respectively'.format(fname_save_responses, fname_save_json))
            pk.dump(dict_res, open(fname_save_responses, 'wb'))
            pk.dump(json_res, open(fname_save_json, 'wb'))

            # pp(res.jres)

            dff.at[aff_name, key_acc] = 1

            for x in dict_res:
                dff = pd_write_data(dff, x, key_acc, 1)

        elif res.jres is None:
            if res.http_error in [401, 429]:
                logger.debug("error retrieved, error is {}".format(res.http_error))
            else:
                logger.debug("error retrieved, error is {}".format(res.http_error))
                dff.at[aff_name, key_acc] = -1


        logger.debug('updating csv file {}'.format(fname_aff_names))
        dff.to_csv(fname_aff_names)
    """
