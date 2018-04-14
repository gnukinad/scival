import pandas as pd
import numpy as np
import pymongo

from model import scopus_metrics


class mongo_scopus_metrics:

    def __init__(self):

        self.client = pymongo.MongoClient('localhost', 27017)
        self.db = self.client['test_metrics']
        self.metrics = self.db['metrics']

        '''
        'scopus_id': <int>,
        'metricType': <string>,
        'valueByYear': {year<int>: value<int>}
        '''

        # idea of the structure
        self.query_template = '''{
        'scopus_id': {scopus_id},
        'metricType': {metricType},
        'valueByYear': {res}
        '''


    def isValid(self, item):

        assert isinstance(item, scopus_metrics), 'only "metrics" class is accepted'

    def update_item_by_year(self, scopus_id, metricType, year, value):

        # i need to preserve the document structure
        # self.isValid()

        self.metrics.update_one({ 'affiliation.scopus_id': scopus_id, 'results.metricType': metricType },
                                {'$set': {
                                    'results.value.{}'.format(year): int(value)
                                }},
                                upsert=True
        )


    def update_item_by_year2(self, **kwargs):

        # i need to preserve the document structure
        self.isValid(scopus_metrics(**kwargs))

        self.metrics.update_one({ 'affiliation.scopus_id': kwargs['scopus_id'],
                                    'results.metricType': kwargs['metricType']
                                  },
                                {'$set': {
                                    'results.value.{}'.format(kwargs['year']): int(kwargs['value'])
                                }},
                                upsert=True
        )

