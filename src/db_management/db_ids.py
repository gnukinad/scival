import pymongo
import pandas as pd
from pprint import pprint as pp
import numpy as np
from model import ids
import re


class MyMongo:

    def __init__(self):

        self.client = pymongo.MongoClient('localhost', 27017)
        self.db = self.client['demo']
        self.aff_ids = self.db['name_ids']


    def isValid(self, item):

        assert isinstance(item, ids), 'only {} is accepted'.format(type(ids))


    def insert_affiliation(self, item):

        # insert an affiliation into database

        assert isinstance(item, ids), 'only {} is accepted'.format(type(ids))

        if not self.aff_ids.find_one(item.to_dict()):
            self.aff_ids.insert_one(item.to_dict())
            print('inserted')
        else:
            print('such id already in the db')
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


    def append_child_aff(self, parent_aff, index_field, child_affs):

        # self.aff_ids.find_one_and_update({index_field: parent_aff.to_dict[index_field]}, {'$push': {"child_id", child_affs}})
        self.aff_ids.find_one_and_update({index_field: parent_aff.to_dict()[index_field]}, {'$push': {'child_id': child_affs}})



def try_partial_input():
# if __name__ == "__main__":

    db_ids = MyMongo()

    old_item = {'scival_id': 508076}
    new_item = {'city': 'Change to something'}

    a = ids(**old_item)
    b = ids(**new_item)

    db_ids.partial_insert(a, 'scopus_id', b)


# def try_insert_child_ids():
if __name__ == "__main__":

    db_ids = MyMongo()

    df = pd.read_excel('aaa.xlsx').rename({'Affiliation ID': 'scopus_id', 'Name': 'name'}, axis=1)

    parent_aff = ids(name='Harvard University', scival_id=508076)

    for index, row in df.iterrows():

        db_ids.append_child_aff(parent_aff, 'scival_id', {'scopus_id': row.scopus_id})
        db_ids.insert_affiliation(ids(**row))


# if __name__ == "__main__":
def transer_ids_from_institution():

    db_ids = MyMongo()

    fname_main = '../../data/universities_table.csv'

    drop_cols = ['order', 'metrics', 'id_downloaded', 'link', 'type', 'name', 'uri']

    df = pd.read_csv(fname_main)

    cols = df.columns.tolist()

    # df = df.set_index("Institution")

    drop_cols.extend([x for x in cols if re.search(r'downloaded', x, re.I)])
    drop_cols = np.unique(drop_cols)

    df = df.drop(drop_cols, axis=1).rename({'id': 'scival_id', 'Institution': 'name'}, axis=1)

    cols = df.columns.tolist()

    for index, row in df.iloc[:5, :].iterrows():

        a = row.to_dict()
        print('index is ', index)
        # print('row is ', row)
        item = ids(**row.to_dict())
        db_ids.insert_affiliation(item)



# if __name__ == "__main__":
def try_insert():

    # inserting item should be a dictionary
    db_ids = MyMongo()

    a = ids(name='Harvard University', id=20123, country='USA', city='Cambridge', child_id=[231, 2123], scopus_id=12)

    db_ids.insert_affiliation(a)
