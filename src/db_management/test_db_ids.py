import pandas as pd
from db_management.model import ids
from db_management.db_ids import mongo_ids
import re
import numpy as np



# if __name__ == "__main__":
def transer_ids_from_institution():

    db_ids = mongo_ids()
    n = 100

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
        print('inserting item')
        print(item)
        db_ids.insert_affiliation(item)



# if __name__ == "__main__":
def try_insert():

    # inserting item should be a dictionary
    db_ids = mongo_ids()

    a = ids(name='Harvard University', country='USA', city='Cambridge')

    db_ids.insert_affiliation(a)


# if __name__ == "__main__":
def try_insert_child_ids():

    db_ids = mongo_ids()

    df = pd.read_excel('aaa.xlsx').rename({'Affiliation ID': 'scopus_id', 'Name': 'name'}, axis=1)

    parent_aff = ids(name='Harvard University', scival_id=508076)

    for index, row in df.iterrows():

        db_ids.append_child_aff(parent_aff, 'scival_id', {'scopus_id': row.scopus_id})
        db_ids.insert_affiliation(ids(**row))
        db_ids.append_aff(ids(**row), 'scopus_id', 'parent_id', {'scival_id': parent_aff.scival_id})





def try_partial_input():
# if __name__ == "__main__":

    db_ids = mongo_ids()

    old_item = {'scival_id': 508076}
    new_item = {'city': 'Change to something'}

    a = ids(**old_item)
    b = ids(**new_item)

    db_ids.partial_insert(a, 'scopus_id', b)



def try_find_and_filter():

    coll = mongo_ids()

    a = coll.filter_children_by_name('parent_id', [{'scival_id': 508076}], 'name', 'harvard')


    return a


if __name__ == "__main__":
# def transer_table_to_db():

    db_name = "ids"
    coll_name = "name_ids_updated5"

    db_ids = mongo_ids(db_name=db_name, coll_name=coll_name)
    # n = 100

    fname_main = '../data/universities_table.csv'

    drop_cols = ['order', 'metrics', 'id_downloaded', 'link', 'type', 'name', 'uri']
    # drop_cols = ['order', 'metrics', 'id_downloaded', 'link', 'type', 'name', 'uri', 'countryCode', 'country']

    df = pd.read_csv(fname_main)

    cols = df.columns.tolist()

    # df = df.set_index("Institution")

    drop_cols.extend([x for x in cols if re.search(r'downloaded', x, re.I)])
    drop_cols = np.unique(drop_cols)

    df = df.drop(drop_cols, axis=1).rename({'id': 'scival_id', 'Institution': 'name'}, axis=1)
    df = df.replace(np.nan, '')

    cols = df.columns.tolist()

    index_field = 'scival_id'

    for index, row in df.iterrows(): 
    # for index, row in df.iloc[-10:, :].iterrows(): 

        if row[index_field]:

            print("inserting item")
            # a = row.to_dict()
            # print('a is ')
            # print(a)

            item = ids(**row.to_dict())
            print(item.to_dict())

            db_ids.partial_insert_by_index(row[index_field], index_field, item)
