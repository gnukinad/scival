import sys
import pymongo
import pandas as pd
from pprint import pprint as pp
import numpy as np
from db_management.model import metric_ack_model
# import model.metric_ack_model as metric_ack_model
from db_management.db_ids import mongo_ids
import re



class mongo_metric_ack:

    # write acknowledgement here

    def __init__ (self, db_name=None, coll_name=None, address=None, port=None):

        if db_name is None:
            db_name = 'acknowledgements'

        if coll_name is None:
            coll_name = 'acks'

        if address is None:
            address = 'localhost'

        if port is None:
            port = 27017

        self.client = pymongo.MongoClient(address, port)
        self.db = self.client[db_name]
        self.aff_acks = self.db[coll_name]

        self.db_ids = mongo_ids()


    def isValid(self, item):

        assert isinstance(item, metric_ack_model), 'only metric_ack_model is accepted'


    def update_item_by_year(self, parent_field, **kwargs):

        # i need to preserve the document structure
        self.isValid(metric_ack_model(**kwargs))

        self.aff_acks.update_one({ 'affiliation.{}'.format(parent_field): kwargs[parent_field],
                                    'ack.metricType': kwargs['metricType']
                                  },
                                {'$set': {
                                    'ack.value.{}'.format(kwargs['year']): int(kwargs['ack'])
                                }},
                                upsert=True
        )


    def find_item(self, parent_field, item_id, metricType, year, ack):

        # find item by metrics
        # self.isValid(metric_ack(scopus_id=scopus_idkwargs))

        a = self.aff_acks.find_one({ 'affiliation.{}'.format(parent_field): item_id,
                                'ack.metricType': metricType,
                                'ack.value.{}'.format(year): ack
        })

        if a:
            return False
        else:
            return True


    def find_valid_ids(self, metricType, year, n):
        # this method searches for ids by scopus, i.e. each one separately

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


    def find_valid_parent_ids(self, metricType, year, n, index_field=None, child_field=None, child_id_field=None):
        # this method searches for ids by scopus, i.e. each one separately

        if index_field is None:
            index_field = 'scival_id'

        if child_field is None:
            child_field = 'scopus_id'

        if child_id_field is None:
            child_id_field = 'child_id'

        all_aff_ids = self.db_ids.aff_ids.find({index_field: {'$exists': True}})

        valid_ids = []

        i = 0

        res = []

        for aff_id in all_aff_ids:

            if i == n:
                break

            if self.find_item(index_field, aff_id[index_field], metricType, year, 1) and child_id_field in list(aff_id.keys()):
                valid_ids = [x[child_field] for x in aff_id[child_id_field]]
                parent_id = aff_id[index_field]


                a = {index_field: aff_id[index_field],
                     child_id_field: valid_ids,
                     'name': aff_id['name']}

                res.append(a)

                # print(aff_id)
                # print(aff_id[index_field])
                i = i + 1


        return res



class mongo_metric_ack_with_query:

    # write acknowledgement here

    def __init__ (self, db_name=None, coll_name=None, address=None, port=None):

        if db_name is None:
            db_name = 'acknowledgements'

        if coll_name is None:
            coll_name = 'acks'

        if address is None:
            address = 'localhost'

        if port is None:
            port = 27017

        self.client = pymongo.MongoClient(address, port)
        self.db = self.client[db_name]
        self.aff_acks = self.db[coll_name]

        self.db_ids = mongo_ids()


    def isValid(self, item):

        assert isinstance(item, metric_ack_model), 'only metric_ack_model is accepted'


    def update_item_by_year(self, parent_field, **kwargs):

        # i need to preserve the document structure
        self.isValid(metric_ack_model(**kwargs))

        assert "queryType" in list(kwargs.keys()), "queryType param in acks is not found"

        self.aff_acks.update_one({ 'affiliation.{}'.format(parent_field): kwargs[parent_field],
                                    'ack.metricType': kwargs['metricType'],
                                    'query.queryType': kwargs['queryType']
                                  },
                                {'$set': {
                                    'ack.value.{}'.format(kwargs['year']): int(kwargs['ack']),
                                    'query.queryType': kwargs['queryType'],
                                }},
                                upsert=True
        )


    def find_item(self, parent_field, item_id, metricType, year, ack, queryType):

        # find item by metrics
        # self.isValid(metric_ack(scopus_id=scopus_idkwargs))

        a = self.aff_acks.find_one({ 'affiliation.{}'.format(parent_field): item_id,
                                'ack.metricType': metricType,
                                'ack.value.{}'.format(year): ack,
                                'query.queryType': queryType
        })

        if a:
            return False
        else:
            return True


    def find_valid_ids(self, metricType, year, n, queryType):
        # this method searches for ids by scopus, i.e. each one separately

        all_scopus_ids = self.db_ids.aff_ids.find({'scopus_id': {'$exists': True}})

        i = 0

        valid_ids = []

        for aff_id in all_scopus_ids:

            if i == n:
                break

            if self.find_item(aff_id['scopus_id'], metricType, year, 1, queryType):
                valid_ids.append(aff_id['scopus_id'])
                i = i + 1

        return valid_ids


    def find_valid_parent_ids(self, metricType, year, n, queryType, index_field=None, child_field=None, child_id_field=None):
        # this method searches for ids by scopus, i.e. each one separately

        if index_field is None:
            index_field = 'scival_id'

        if child_field is None:
            child_field = 'scopus_id'

        if child_id_field is None:
            child_id_field = 'child_id'

        all_aff_ids = self.db_ids.aff_ids.find({index_field: {'$exists': True}})

        valid_ids = []

        i = 0

        res = []

        for aff_id in all_aff_ids:

            if i == n:
                break

            if self.find_item(index_field, aff_id[index_field], metricType, year, 1, queryType) and child_id_field in list(aff_id.keys()):
                valid_ids = [x[child_field] for x in aff_id[child_id_field]]
                parent_id = aff_id[index_field]


                a = {index_field: aff_id[index_field],
                     child_id_field: valid_ids,
                     'name': aff_id['name']}

                res.append(a)

                # print(aff_id)
                # print(aff_id[index_field])
                i = i + 1


        return res


    def find_valid_parent_ids2(self, metricType, year, n, queryType, index_field=None, child_field=None, child_id_field=None, non_valid_acks=None):
        # this method searches for ids by scopus, i.e. each one separately

        if index_field is None:
            index_field = 'scival_id'

        if child_field is None:
            child_field = 'scopus_id'

        if child_id_field is None:
            child_id_field = 'child_id'

        if non_valid_acks is None:
            # due to bad_request, due to unknown  error, already retrieved
            non_valid_acks = [-2, -1, 1]


        all_aff_ids = list(self.db_ids.aff_ids.find({index_field: {'$exists': True}}))

        valid_ids = []

        i = 0

        res = []

        for ii, aff_id in enumerate(all_aff_ids):

            if i == n:
                break

            # if child_id_field was exctracted before
            if child_id_field in list(aff_id.keys()):

                field_name = 'affiliation.{}'.format(index_field)

                item = self.aff_acks.find_one({
                    field_name: aff_id[index_field],
                    'ack.metricType': metricType,
                    'query.queryType': queryType,
                    'ack.value.{}'.format(year): {"$exists": True}
                })


                if item:
                    ack = item['ack']['value']['{}'.format(year)]

                    if ack in non_valid_acks:
                        continue

                # 'ack.value.{}'.format(year): ack,

                valid_ids = [x[child_field] for x in aff_id[child_id_field]]
                parent_id = aff_id[index_field]


                a = {index_field: aff_id[index_field],
                     child_id_field: valid_ids,
                     'name': aff_id['name']}

                res.append(a)

                # print(aff_id)
                # print(aff_id[index_field])
                i = i + 1


        return res
