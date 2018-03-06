import pandas as pd
import re
from difflib import SequenceMatcher
import numpy as np
import pycountry as pc
from pprint import pprint as pp


def list_strip(a):

    if isinstance(a, list):
        return [x.strip() for x in a]
    else:
        return a.strip()


def remove_long_items(a, length=None):

    if length is None:
        length = 2

    if isinstance(a, list):
        return [x for x in a if len(x) > length]
    else:
        if len(a) > length:
            return a
        else:
            pass


def pop_item_from_list(a, item):

    [a.pop(a.index(x)) for x in item]




if __name__ == "__main__":

    maplist = '../data/aff_name_map_list.xlsx'
    aff_names = '../data/universities_table.csv'
    nobel_prize = '../data/prize/nobel_prize.xlsx'

    df_map = pd.read_excel(maplist)
    df = pd.read_csv(aff_names)
    df_nobel = pd.read_excel(nobel_prize)

    bad_aff = []
    bad_ind = []

    new_df = pd.DataFrame(columns=['nobel_share', 'year', 'name', 'nobel_field'])
    # affillation	family_name	first_name	link	prize_motivation	prize_share	year	field

    aaa = []

    # for index, row in df_nobel.iloc[:20,:].iterrows():
    for index, row in df_nobel.iterrows():
        # print('row is ', row)

        try:
            nobel_name = row['affillation']
            if nobel_name != 'nan':
                print(nobel_name)
                aff_from_map = df_map.groupby('aff_name').get_group(nobel_name)['metrics_aff_name']
                a = np.unique(aff_from_map.values).tolist()[0]

                if isinstance(a, str):

                    aaa.append(pd.DataFrame({'nobel_share': [row['prize_share']],
                                             'year': [row['year']],
                                             'name': [a],
                                             'nobel_field': [row['field']]}))

                elif isinstance(a, int):
                    print('nobel_name is ', nobel_name)
                    print(a)


        except Exception as e:

            # print(e)
            bad_ind.append(index)
            bad_aff.append(nobel_name)

    new_df = pd.concat(aaa, ignore_index=True)
