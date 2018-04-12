import pandas as pd
import numpy as np
import os


BASE_DIR = os.path.abspath(os.path.realpath(__file__))
BASE_DIR = os.path.join(os.path.dirname(BASE_DIR), '..')
os.chdir(BASE_DIR)

FOLNAME_AFF_SEARCH = os.path.join(BASE_DIR, 'data', 'aff_search')
FOLNAME_METRIC_RESPONSE = os.path.join(BASE_DIR, 'data', 'metric_response')

if __name__ == "__main__":

    fname_df_order = os.path.join('data', 'universities_order.xlsx')
    fname_df_aff   = os.path.join('data', 'universities_table.csv')

    df_order = pd.read_excel(fname_df_order).set_index('name').replace(np.nan, '')
    df_aff = pd.read_csv(fname_df_aff).set_index("Institution")

    if 'order' not in df_order.columns.tolist():
        df_order = df_order.assign(order=range(df_order.shape[0]))

    if 'order' not in df_aff.columns.tolist():
        df_aff = df_aff.assign(order=0)

    df_order_index = df_order.index.tolist()

    maxv = np.max(df_order['order'].tolist())
    i = 1

    # for index, row in df_aff.iloc[:200, :].iterrows():
    for index, row in df_aff.iterrows():

        if index in df_order_index:

            print('before applying order ', df_order.at[index, 'order'])
            print('index is ', index, '\tvalue is ', df_order.at[index, 'order'])

            if df_order.at[index, 'order'] == '':

                print('epmty')

                df_aff.at[index, 'order'] = maxv + i
                i = i + 1
            else:

                print('not empty')
                df_aff.at[index, 'order'] = df_order.at[index, 'order']


            print('after applying order ', df_order.at[index, 'order'])
            print()

    df_aff.to_csv(fname_df_aff)
