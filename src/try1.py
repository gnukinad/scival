import numpy as np
import pandas as pd
import pymongo
import os
import errno
import logging
from my_scival import InstitutionSearch, MetricSearch

from urllib import parse, request
from urllib.error import HTTPError
from func import get_aff_id

BASE_DIR = os.path.join(os.getenv('HOME'), "projects", "scival")
os.chdir(BASE_DIR)

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


# if __name__=="__main__":
def main1():

    MY_API_KEY = "e53785aedfc1c54942ba237f8ec0f891"
    # MY_API_KEY = None

    # apiKey = "e53785aedfc1c54942ba237f8ec0f891"

    # logger.debug("creating a class")
    # a = InstitutionSearch(["Harvard University"], MY_API_KEY)

    aff_id = [508175, 508076]


    logger.debug("creating a metrics class")
    m = MetricSearch(aff_id=aff_id, apiKey = MY_API_KEY)

    logger.debug("encoding the metrics")
    m.encode()
    print("request is ")
    print(m.parsed_url)
    m.send_request()


    all_metrics = ["Collaboration", "CitationCount", "CitationPerPublication", "CollaborationImpact", "CitedPublications", "FieldWeightedCitationImpact", "hIndices", "ScholarlyOutput", "PublicationsInTopJournalPercentiles", "OutputsInTopCitationPercentiles"]

    ma = ",".join(all_metrics)


# trying to retrieve the id of the university
if __name__=="__main__":

    # read the name
    # send the request
    # get the id

    logger.debug('loading university names')
    fname_aff_names = os.path.join(BASE_DIR, "universities_table.csv")

    df = pd.read_csv(fname_aff_names)
    key_aff = 'Institution'
    key_acc = 'downloaded'

    a = df.loc[:, key_acc] == 0
    all_aff_names = df.loc[a, key_aff].tolist()

    n = 1

    logger.debug('starting to load institution_id')
    for i in range(n):

        aff_name = all_aff_names[i]
        logger.debug('aff_name is {}'.format(aff_name))

        MY_API_KEY = "e53785aedfc1c54942ba237f8ec0f891"

        res = get_aff_id(aff_name, MY_API_KEY)


# def get_aff_id(aff_name, api_key):


        # a = InstitutionSearch(["Harvard University"], MY_API_KEY)
