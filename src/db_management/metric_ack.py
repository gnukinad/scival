import sys
print(sys.path)


import pymongo
import pandas as pd
from pprint import pprint as pp
import numpy as np
from db_management.model import metric_ack_model
# import model.metric_ack_model as metric_ack_model
from db_management.db_ids import mongo_ids
import re




class mongo_metric_ack():

    # write acknowledgement here

    def __init__ (self):

        self.client = pymongo.MongoClient('localhost', 27017)
        self.db = self.client['acknowledgements']
        self.aff_acks = self.db['acks']

        self.db_ids = mongo_ids()


    def isValid(self, item):

        assert isinstance(item, metric_ack_model), 'only metric_ack_model is accepted'


    def update_item_by_year(self, **kwargs):

        # i need to preserve the document structure
        self.isValid(metric_ack_model(**kwargs))

        self.aff_acks.update_one({ 'affiliation.scopus_id': kwargs['scopus_id'],
                                    'ack.metricType': kwargs['metricType']
                                  },
                                {'$set': {
                                    'ack.value.{}'.format(kwargs['year']): int(kwargs['ack'])
                                }},
                                upsert=True
        )
        print('finished writing')

    def find_item(self, scopus_id, metricType, year, ack):

        # find item by metrics
        # self.isValid(metric_ack(scopus_id=scopus_idkwargs))

        a = self.aff_acks.find_one({ 'affiliation.scopus_id': scopus_id,
                                'ack.metricType': metricType,
                                'ack.value.{}'.format(year): ack
        })

        if a:
            return False
        else:
            return True


    def find_valid_ids(self, metricType, year, n):

        all_scopus_ids = self.db_ids.aff_ids.find({'scopus_id': {'$exists': True}})

        i = 0

        valid_ids = []

        for aff_id in all_scopus_ids:

            if i == n:
                break

            if self.find_item(aff_id['scopus_id'], metricType, year, 1):
                valid_ids.append(aff_id['scopus_id'])
                i = i + 1

        return valid_ids
