import pymongo
import pandas as pd
from pprint import pprint as pp
import numpy as np
from db_management.model import ids
import re



class mongo_ids:

    def __init__(self, db_name=None, coll_name=None):

        if db_name is None:
            db_name = "ids"

        if coll_name is None:
            coll_name = "name_ids_updated2"

        self.client = pymongo.MongoClient('localhost', 27017)
        self.db = self.client[db_name]
        self.aff_ids = self.db[coll_name]


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


    def partial_insert_by_index(self, index_value, index_field, new_item):

        # insert item partially, the position is selected by index_field
        self.isValid(new_item)

        # a = item.to_dict()[index_field]

        for key, value in new_item.to_dict().items():


            if key != index_field:
                print("inserting items")
                self.aff_ids.update_one({index_field: index_value, key: {'$exists': False}}, {'$set': {key: value}}, upsert=True)


    def append_child_aff(self, parent_aff, index_field, child_aff):

        # self.aff_ids.find_one_and_update({index_field: parent_aff.to_dict[index_field]}, {'$push': {"child_id", child_affs}})
        self.aff_ids.find_one_and_update({index_field: parent_aff.to_dict()[index_field]}, {'$addToSet': {'child_id': child_aff}})


    def append_aff(self, parent_aff, index_field, append_field, append_value):

        self.aff_ids.find_one_and_update({index_field: parent_aff.to_dict()[index_field]}, {'$addToSet': {append_field: append_value}})


    def append_affs(self, parent_aff, index_field, append_field, append_values):

        # add a list of append_values alltogether
        self.aff_ids.find_one_and_update({index_field: parent_aff.to_dict()[index_field]}, {'$addToSet': {append_field: {'$each': append_values}}})


    def filter_children_by_name(self, parent_field, parent_id, filter_field, filter_name):

        # filter child ids by name
        return self.aff_ids.find({parent_field: parent_id, filter_field: {'$regex': filter_name, '$options': 'i'}})


