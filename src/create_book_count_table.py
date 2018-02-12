import json
import pandas as pd
import numpy as np
import pickle as pk
from os.path import join as opj
import os
from pprint import pprint as pp

dname = opj('..', 'data', 'aff_book_count')


def retrieve_book_count(d):

    query = d['search-results']['opensearch:Query']['@searchTerms']
    terms = query.split("AND")

    ind = [i for i in range(len(terms)) if 'affilorg' in terms[i]][0]

    aff_name = terms[ind].strip()[10:-2]     # text between brackets, e.g. affilorg("Harvard University")

    return {'name': aff_name,
            "book_count": d['search-results']['opensearch:totalResults']}


if __name__ == "__main__":

    # a = pk.load(open('data/aff_book_count'
    df = pd.DataFrame()

    for root, dirs, files in os.walk(dname):

        for x in files:
            fname = opj(root, x)

            t = open(fname, 'r').readlines()[0]
            d = json.loads(t)
            bc = retrieve_book_count(d)
            df = df.append(bc, ignore_index=True)

    df.to_csv(opj(dname, '..', 'book_count_2012.csv'), index=False)
