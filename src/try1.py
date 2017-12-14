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

BASE_DIR = os.path.join(os.getenv('HOME'), "projects", "scival")
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

        if aux_key is not None and aux_value is not None:
            df.at[d['name'], aux_key] = aux_val

    return df



# if __name__=="__main__":
def main1():

    MY_API_KEY = "e53785aedfc1c54942ba237f8ec0f891"
    # MY_API_KEY = None

    # apiKey = "e53785aedfc1c54942ba237f8ec0f891"

    # logger.debug("creating a class")
    # a = InstitutionSearch(["Harvard University"], MY_API_KEY)

    # aff_id = [508175, 508076]
    aff_id = [508175]


    logger.debug("creating a metrics class")
    m = MetricSearch(aff_id=aff_id, apiKey = MY_API_KEY)

    m.get_jres()


    all_metrics = ["Collaboration", "CitationCount", "CitationPerPublication", "CollaborationImpact", "CitedPublications", "FieldWeightedCitationImpact", "hIndices", "ScholarlyOutput", "PublicationsInTopJournalPercentiles", "OutputsInTopCitationPercentiles"]

    ma = ",".join(all_metrics)


# trying to retrieve the id of the university
# if __name__=="__main__":
def main2():

    """
    get the InstitutionSearch output for every affiliation that was not already downloaded
    """

    MY_API_KEY = "e53785aedfc1c54942ba237f8ec0f891"
    # MY_API_KEY = "7f59af901d2d86f78a1fd60c1bf9426a"

    logger.debug('loading university names')
    fname_aff_names = os.path.join(BASE_DIR, 'data', "universities_table.csv")

    df = pd.read_csv(fname_aff_names)

    a = df.loc[:, key_acc] == 0     # which universities are not downloaded
    all_aff_names = df.loc[a, key_aff].tolist()

    n = 2
    responses = []
    jsons = []

    dff = df.copy()
    dff = dff.set_index(key_aff).replace(0, '')

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

        fname_save_responses = os.path.join(AFF_FNAME_SEARCH, fname_save_responses)
        fname_save_json = os.path.join(AFF_FNAME_SEARCH, fname_save_json)

        logger.debug('saving responses and json response to {} and {} respectively'.format(fname_save_responses, fname_save_json))
        pk.dump(dict_res, open(fname_save_responses, 'wb'))
        pk.dump(json_res, open(fname_save_json, 'wb'))

        for x in dict_res:
            dff = pd_write_data(dff, x, key_acc, 1)

        logger.debug('updating csv file {}'.format(fname_aff_names))
        dff.to_csv(fname_aff_names)


if __name__ == "__main__":

    """
    get metric responses for each affiliation id and write to csv
    """

    MY_API_KEY = "e53785aedfc1c54942ba237f8ec0f891"

    logger.debug("getting the affilitation id")


    # MY_API_KEY = "7f59af901d2d86f78a1fd60c1bf9426a"

    logger.debug('loading university names')
    fname_aff_names = os.path.join(BASE_DIR, 'data', "universities_table.csv")

    df_aff = pd.read_csv(fname_aff_names).set_index(key_aff)

    aff_with_aff_id = df_aff.loc[df_aff.loc[:, key_acc] == 1, key_id].dropna().tolist()   # which universities have ids
    aff_with_metric = df_aff.loc[df_aff.loc[:, key_met] == 1, key_id].dropna().tolist()   # which universities have metrics

    aff_ids = [int(x) for x in list(set(aff_with_aff_id) - set(aff_with_metric))]
    
    all_inds = df_aff.index.tolist()

    n = 2
    responses = []
    jsons = []

    for i in range(n):
        aff_id = aff_ids[i]

        # aff_ind = df_aff.reset_index().set_index(key_id).index[aff_id]

        a = df_aff.reset_index().set_index(key_id).at[aff_id, key_aff]
        print(a)
        # aff_ind = df_aff.reset_index().set_index(key_id).index[aff_id]

        # logger.debug('getting the response for id and name {} and {}'.format(aff_id, df_aff.at[aff_id,key_aff]))

        # logger.debug('starting to load institution_id')

        """
        m = MetricSearch(aff_id=aff_id, apiKey = MY_API_KEY)

        m.get_jres()

        res = m.jres

        try:
            logger.debug('getting the data frame of the results')
            df = get_aff_metrics(res['results'])
            logger.debug('metrics dataframe was created successfully')

            logger.debug('saving metrics dataframe for id {}'.format(aff_id))
            fname_metrics = os.path.join(FOLNAME_METRIC_RESPONSE, "id{}".format(aff_id))
            logger.debug('saving metrics for id {} dataframe'.format(aff_id))
            df.to_csv(fname_metrics)
            logger.debug('metrics dataframe for id {} was saved sucessfully'.format(aff_id))

            df_aff.at[aff_ind, key_met] = 1
        except:
            df_aff.at[aff_ind, key_met] = -1
            df_aff.at[aff_ind, key_acc] = -1

        df_aff.to_csv(fname_aff_names)
        logger.debug('{} was updated successfully'.format(fname_aff_names))
        """
