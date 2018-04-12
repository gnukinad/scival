import numpy as np
import pandas as pd
import pymongo
import os
import errno
import logging
from my_scival import InstitutionSearch, MetricSearch

from urllib import parse, request
from urllib.error import HTTPError
from func import get_InstitutionSearch, get_aff_id, get_aff_metrics

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

class my_df():

    def __init__(self, df):

        self.df = df.copy()
        self.i = 0
        self.inds = self.df.index.tolist()
        self.n_inds = len(self.inds)


    def get_iterrow(self):
        """iterate through df to get next row"""

        if self.i < self.n_inds:
            ind = self.inds[self.i]
            row = self.df.loc[ind, :]

            self.i = self.i + 1

            return row

        else:
            print("sorry, reached the end of DataFrame")
            return -1


    def get_next_id(self):
        """get next id that needs a metrics to be downloaded"""

        while(True):

            row = self.get_iterrow()

            if isinstance(row, int) is False:
                if row.id_downloaded == 1 and (np.isnan(row.metrics) == True or row.metrics == 0):
                    id = row.id
                    name = row.name
                    break
            elif isinstance(row, int):
                if row == -1:
                    id = -1
                    name = -1
                    raise("End of DataFrame")

                break

        return id, name


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


def get_valid_aff_names(df_iter, n):

    for i in range(n):

        # get university names
        try:
            aff_id, aff_name = df_iter.get_next_id()
            
            aff_id = np.int(aff_id)
            
            aff_ids.append(aff_id)
            aff_names.append(aff_name)
        except:
            logger.debug("error at retrieving university id, aff_name is {}".format(aff_name))
            logger.debug("updating the table")
            df_aff.at[aff_name, key_met] = -1

        affs = dict(zip(aff_ids, aff_names))

    return affs



# trying to retrieve the id of the university
if __name__ == "__main__":

    """
    get metric responses for each affiliation id and write to csv
    """

    MY_API_KEY = "e53785aedfc1c54942ba237f8ec0f891"
    # MY_API_KEY = "7f59af901d2d86f78a1fd60c1bf9426a"

    logger.debug('loading university names')
    fname_aff_names = os.path.join(BASE_DIR, 'data', "universities_table.csv")

    df_aff = pd.read_csv(fname_aff_names).set_index(key_aff)

    n = 15

    df_iter = my_df(df_aff)

    aff_ids = []
    aff_names = []

    affs = get_valid_aff_names(df_iter, n)

    aff_id = list(affs.keys())

    m = MetricSearch(aff_id=aff_id, apiKey=MY_API_KEY)

    logger.debug('starting to load institution metrics')

    m.get_jres()
    res = m.jres

    try:
        logger.debug('getting the data frame of the results')
        df = get_aff_metrics(res['results'])
        logger.debug('metrics dataframe was created successfully')

        logger.debug('saving metrics dataframe for id {}'.format(aff_id))
        fname_metrics = os.path.join(FOLNAME_METRIC_RESPONSE, "id{}.csv".format(aff_id))
        logger.debug('saving metrics for id {} dataframe'.format(aff_id))
        df.to_csv(fname_metrics)
        logger.debug('metrics dataframe for id {} was saved sucessfully'.format(aff_id))

        for x in df.reset_index().name.unique():
            logger.debug('updating the acknowledgement key in table for {}'.format(x))
            df_aff.at[x, key_met] = 1

    except:

        if res.http_error in [401, 429]:
            logger.debug("error retrieved, error is {}".format(res.http_error))
        else:
            logger.debug("error retrieved, error is {}".format(res.http_error))
            df_aff.at[aff_name, key_acc] = -1

    df_aff.to_csv(fname_aff_names)
    logger.debug('{} was updated successfully'.format(fname_aff_names))
