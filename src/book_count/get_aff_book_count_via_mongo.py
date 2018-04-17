# from scopus import ScopusSearch
import os
import pandas as pd
import numpy as np
import logging
from urllib import parse, request
import urllib.error
import json
from pprint import pprint as pp

import sys

from db_management.metric_ack import mongo_metric_ack
from db_management.scopus_metrics import mongo_scopus_metrics

from func import read_credentials


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


class my_scopus_search():

    def __init__(self, query=None, apiKey=None, httpAccept=None, logger=None):

        httpAccept = httpAccept or "application/json"
        apiKey = apiKey or "7f59af901d2d86f78a1fd60c1bf9426a"

        if query is None:
            raise("query is not given, please, give it")

        self.query = query
        self.httpAccept = httpAccept
        self.apiKey = apiKey
        self.logger = logger or logging.getLogger(__name__)
        self.response = None
        self.jres = None

        self.logger.debug("my_scopus_search class was initialized")
        self.__url = "https://api.elsevier.com/content/search/scopus"
        self.http_error = None


    def encode(self):
        """encode query"""

        self.parsed_url = self.__url + "?query=" + parse.quote_plus(self.query, safe='()')
        self.parsed_url = "&".join([self.parsed_url, "apiKey={}".format(self.apiKey), "httpAccept={}".format(parse.quote(self.httpAccept, safe=""))])

        self.logger.debug("encoding query to {}".format(self.parsed_url))
        return self


    def send_request(self):
        """send request"""

        response = None

        try:
            self.logger.debug("sending the request")
            response = request.urlopen(self.parsed_url)

            self.logger.debug("request retrieved sucessully")
        except urllib.error.HTTPError as e:

            self.http_error = e.code

            if e.code == 404:
                self.logger.warning("NOT FOUND")
                self.logger.warning("error is {}".format(e))
            elif e.code == 400:
                self.logger.warning("invalid request, try again")
                self.logger.warning("error is {}".format(e))
            elif e.code == 401:
                self.logger.warning("cannot be authentified due to missing/invalid credentials")
                self.logger.warning("error is {}".format(e))
            elif e.code == 429:
                self.logger.warning("quota exceeded")
                self.logger.warning("error is {}".format(e))
            else:
                self.logger.warning("unkown error")
                self.logger.warning("error code is {}".format(e))

        except Exception as e:

            response = None
            self.logger.warning("request retrieved with error, the error code is ", e)

        self.response = response
        return self


    def read_json(self):

        if self.response is not None:
            output = json.loads(self.response.read())
            self.jres = output


    def get_search_object(self):

        self.logger.debug('start encoding')
        self.encode()
        self.logger.debug('encoding finished')

        self.send_request()
        self.read_json()

        return self.jres



class scopus_query:

    """in this class I define those keys I need to use to extract number of books for each university
    this means that this class is applicable only in certain cases

    for more instructions, please, read this:
    https://dev.elsevier.com/tips/ScopusSearchTips.htm

    """

    def __init__(self, year=None, afid=None, doctype=None):

        self.q = dict()
        self.query = None   # a final string the query
        self.set_values(year=year, afid=afid, doctype=doctype)


    def set_values(self, year=None, afid=None, doctype=None):

        if year is None:
            year = 2012

        if afid is None:
            afid = 'Harvard University'

        if doctype is None:
            doctype = "bk"

        self.afid = afid
        self.doctype = doctype
        self.year = year
        self.q = dict()
        self.query = None

        return self


    def encode(self):
        self.q['afid']   = 'af-id({})'.format(self.afid)
        self.q['doctype'] = "doctype({})".format(self.doctype)
        self.q['year']    = "pubyear = {}".format(self.year)

        # sample query with af-id
        # https://api.elsevier.com/content/search/scopus?query=af-id(60000650)%20and%20pubyear%20%3D%202016%20and%20doctype(bk)&apiKey=e53785aedfc1c54942ba237f8ec0f891

        return self


    def encode_many(self):
        # encode many ids via OR operator
        self.q['doctype'] = "doctype({})".format(self.doctype)
        self.q['year']    = "pubyear = {}".format(self.year)
        self.q['afid']    = '( ' + ' or '.join(['af-id({})'.format(x) for x in self.afid]) + ' )'

        return self


    def set_query(self):

        self.query = " AND ".join([self.q[key] for key in self.q.keys()])
        self.query.encode()

        return self

    def get_query(self):
        return self.query



if __name__ == "__main__":

    db_name_ack = 'acknowledgements'
    coll_name_ack = 'acks_by_scival'

    db_name_metrics = 'scopus_metrics'
    coll_name_metrics = 'metrics_by_scival'

    parent_field = 'scival_id'
    child_field = 'scopus_id'
    child_id_field = 'child_id'

    coll_ack = mongo_metric_ack(db_name=db_name_ack, coll_name=coll_name_ack)
    coll_metrics = mongo_scopus_metrics(db_name=db_name_metrics, coll_name=coll_name_metrics)

    # MY_API_KEY = "e53785aedfc1c54942ba237f8ec0f891"
    MY_API_KEY = read_credentials("MY_API_KEY")[0]

    _, year, n = sys.argv
    year = int(year)
    n = int(n)

    doctype = "bk"

    # where to save the results
    dname_book_count = os.path.join(BASE_DIR, "data", "aff_book_{}".format(year))

    col_dict = {2012: "book_downloaded_2012",
                2013: "book_downloaded_2013",
                2014: "book_downloaded_2014",
                2015: "book_downloaded_2015",
                2016: "book_downloaded_2016"}


    metricType = 'BookCount'

    valid_dicts = coll_ack.find_valid_parent_ids(metricType, str(year), n)
    # print(valid_ids)
    # print('length is ', len(valid_ids))

    for i in range(n):

        logger.debug("getting univerity name")
        parent_id = valid_dicts[i][parent_field]
        aff_id = valid_dicts[i][child_id_field]
        logger.debug("affiliation id is {}".format(aff_id))

        logger.debug("creating query")
        query = scopus_query(year=year, doctype=doctype, afid=aff_id).encode_many().set_query().get_query()

        logger.debug("query is {}".format(query))

        logger.debug("sending the request")

        fname_jres = os.path.join(dname_book_count, "{}_year_{}.json".format(parent_id, year))

        q = {parent_field: parent_id,
            'metricType': metricType,
            'year': year}

        metric_response = q.copy()
        ack_response = q.copy()


        try:
            res = my_scopus_search(query=query, apiKey=MY_API_KEY)
            logger.debug("respond received sucessfuly")

            s = res.get_search_object()

            # saving response and ack
            metric_response['value'] = s['search-results']['opensearch:totalResults']
            ack_response['ack'] = 1


            logger.debug("updating metrics and ack dbs")
            coll_metrics.update_item_by_year(parent_field, **metric_response)
            coll_ack.update_item_by_year(parent_field, **ack_response)


            logger.debug('saving the response to {}'.format(fname_jres))
            with open(fname_jres, "w") as f:
                json.dump(s, f)

            logger.debug("json file saved successfully")


        except Exception as e:

            if res.http_error == 401:
                logger.debug("error retrieved, error is {}".format(res.http_error))
            elif res.http_error == 429:
                logger.info("quota is exceeded, terminating the program")
                break
            else:
                logger.warning("error retrieved, error is {}".format(res.http_error))

                logger.warning('updating the ack db')
                ack_response['ack'] = -1
                coll_ack.update_item_by_year(parent_field, **ack_response)

            logger.warning('respond has failed, error is ', e)

    try:
        logger.debug("saving the updated table")
        logger.debug("table was updated successfully")
    except Exception as e:
        logger.warn("saving updated talbe has failed, please, save the table manually")
