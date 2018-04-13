import pandas as pd
from model import ids
from db_ids import MyMongo
import re
import numpy as np



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

    a = ids(name='Harvard University', country='USA', city='Cambridge')

    db_ids.insert_affiliation(a)


# if __name__ == "__main__":
def try_insert_child_ids():

    db_ids = MyMongo()

    df = pd.read_excel('aaa.xlsx').rename({'Affiliation ID': 'scopus_id', 'Name': 'name'}, axis=1)

    parent_aff = ids(name='Harvard University', scival_id=508076)

    for index, row in df.iterrows():

        db_ids.append_child_aff(parent_aff, 'scival_id', {'scopus_id': row.scopus_id})
        db_ids.insert_affiliation(ids(**row))
        db_ids.append_aff(ids(**row), 'scopus_id', 'parent_id', {'scival_id': parent_aff.scival_id})





def try_partial_input():
# if __name__ == "__main__":

    db_ids = MyMongo()

    old_item = {'scival_id': 508076}
    new_item = {'city': 'Change to something'}

    a = ids(**old_item)
    b = ids(**new_item)

    db_ids.partial_insert(a, 'scopus_id', b)
