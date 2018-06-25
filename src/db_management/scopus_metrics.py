import pandas as pd
import numpy as np
import pymongo

from db_management.model import scopus_metrics


class mongo_scopus_metrics:

    def __init__ (self, db_name=None, coll_name=None, address=None, port=None):

        if db_name is None:
            db_name = 'scopus_metrics'

        if coll_name is None:
            coll_name = 'metrics'

        if address is None:
            address = 'localhost'

        if port is None:
            port = 27017

        self.client = pymongo.MongoClient(address, port)
        self.db = self.client[db_name]
        self.metrics = self.db[coll_name]

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


    def update_item_by_year(self, parent_field,  **kwargs):

        # i need to preserve the document structure
        self.isValid(scopus_metrics(**kwargs))

        self.metrics.update_one({ 'affiliation.{}'.format(parent_field): kwargs['{}'.format(parent_field)],
                                  'results.metricType': kwargs['metricType']
                                  },
                                {'$set': {
                                    'results.value.{}'.format(kwargs['year']): int(kwargs['value'])
                                }},
                                upsert=True
        )



class mongo_scopus_metrics_with_query:

    def __init__ (self, db_name=None, coll_name=None, address=None, port=None):

        if db_name is None:
            db_name = 'scopus_metrics'

        if coll_name is None:
            coll_name = 'metrics'

        if address is None:
            address = 'localhost'

        if port is None:
            port = 27017

        self.client = pymongo.MongoClient(address, port)
        self.db = self.client[db_name]
        self.metrics = self.db[coll_name]

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


    def update_item_by_year(self, parent_field,  **kwargs):

        # i need to preserve the document structure
        # self.isValid(scopus_metrics(**kwargs))

        print('kwargs')
        print(kwargs)

        print('kwargs.keys()')
        print(kwargs.keys())

        assert "query" in list(kwargs.keys()), "query param metrics is not found"
        assert "queryType" in list(kwargs.keys()), "queryType param in metrics is not found"


        print('updating db')
        self.metrics.update_one({ 'affiliation.{}'.format(parent_field): kwargs['{}'.format(parent_field)],
                                  'results.metricType': kwargs['metricType'],
                                  'query.queryType': kwargs['queryType']
                                  },
                                {'$set': {
                                    'results.value.{}'.format(kwargs['year']): int(kwargs['value']),
                                    'query.queryType': kwargs['queryType'],
                                    'query.query': kwargs['query']
                                }},
                                upsert=True
        )

        print("metrics db updated succesfully")
