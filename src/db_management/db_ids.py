import pymongo
import pandas as pd
from pprint import pprint as pp
import numpy as np
from model import ids
import re



class mongo_ids:

    def __init__(self):

        self.client = pymongo.MongoClient('localhost', 27017)
        self.db = self.client['ids']
        self.aff_ids = self.db['name_ids']


    def isValid(self, item):

        assert isinstance(item, ids), 'only {} is accepted'.format(type(ids))


    def insert_affiliation(self, item):

        # insert an affiliation into database

        assert isinstance(item, ids), 'only {} is accepted'.format(type(ids))

        if not self.aff_ids.find_one(item.to_dict()):
            self.aff_ids.insert_one(item.to_dict())
        else:
            return item



    def insert_affiliations(self, affs):
        # insert a bulk of affiliations

        return [self.insert_affiliation(x) for x in affs]


    def partial_insert(self, item, index_field, new_item):

        # insert item partially, the position is selected by index_field
        self.isValid(item)
        self.isValid(new_item)

        a = item.to_dict()[index_field]

        for key, value in new_item.to_dict().items():
            self.aff_ids.update_one({index_field: a, key: {'$exists': True}}, {'$set': {key: value[0]}}, upsert=False)


    def append_child_aff(self, parent_aff, index_field, child_aff):

        # self.aff_ids.find_one_and_update({index_field: parent_aff.to_dict[index_field]}, {'$push': {"child_id", child_affs}})
        self.aff_ids.find_one_and_update({index_field: parent_aff.to_dict()[index_field]}, {'$addToSet': {'child_id': child_aff}})


    def append_aff(self, parent_aff, index_field, append_field, append_value):

        self.aff_ids.find_one_and_update({index_field: parent_aff.to_dict()[index_field]}, {'$addToSet': {append_field: append_value}})


    def append_affs(self, parent_aff, index_field, append_field, append_values):

        # add a list of append_values alltogether
        self.aff_ids.find_one_and_update({index_field: parent_aff.to_dict()[index_field]}, {'$addToSet': {append_field: {'$each': append_values}}})
