import pandas as pd
import numpy as np
import os
import glob

import logging
from pprint import pprint as pp

BASE_DIR = os.path.abspath(os.path.realpath(__file__))
BASE_DIR = os.path.join(os.path.dirname(BASE_DIR), '..')
os.chdir(BASE_DIR)

FOLNAME_AFF_SEARCH = os.path.join(BASE_DIR, 'data', 'aff_search')
FOLNAME_METRIC_RESPONSE = os.path.join(BASE_DIR, 'data', 'metric_response')

key_aff = 'Institution'
key_acc = 'id_downloaded'
key_id  = 'id'
key_met = 'metrics'

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create file handler which logs info messages
fh = logging.FileHandler(os.path.join(BASE_DIR, 'logs', 'logs.txt'), 'w', 'utf-8')
fh.setLevel(logging.DEBUG)

# create console handler with a debug log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# creating a formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-8s: %(message)s')

# setting handler format
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)


mfwci = "FieldWeightedCitationImpact"
mcol_impact = "CollaborationImpact"
mcol = "Collaboration"
mcit_pubs = "CitedPublications"
mcit_per_pub = "CitationsPerPublication"
mcit = "CitationCount"
mscholar_output = "ScholarlyOutput"
mtjp = "PublicationsInTopJournalPercentiles"   # tjp - top journal percentiles
mocp = "OutputsInTopCitationPercentiles"       # ocp - outputs in citation percentiles


metrics = [mfwci, mcol, mcol_impact, mcit, mcit_pubs, mcit_per_pub, mscholar_output, mtjp, mocp]


cname = 'name'
cmetric = 'metricType'
cid = 'id'
ccol_type = 'collabType'
cval = 'valueByYear'
cpcent = 'percentageByYear'
cthresh = 'threshold'
cyear = 'year'

col_single = 'Single'
col_inter =  'International'
col_nat = 'National'
col_ins = 'Institutional'


def transform_pd(df):

    name = df.name.unique()
    id = df.id.unique()

    inds = df.index.tolist()

    a = {'name': name, 'id': id}

    for x in inds:

        row = df.loc[x, :]
    
        m = row[cmetric]
        y = row[cyear]
        
        if m in [mcol_impact, mcol]:
            col_type = row[ccol_type]
            val = row[cval]
            
            t = "{}_{}_{}".format(m, col_type.title().replace(" ", ""), y)
            
            a[t] = val
            
        elif m in [mocp, mtjp]:
            
            threshold = row[cthresh]
            val = row[cval]
            pval = row[cpcent]
            
            tval = "{}_{}_{}_{}".format(m, int(threshold), y, "value")
            tpcent = "{}_{}_{}_{}".format(m, int(threshold), y, "percentage")
            
            a[tval] = val
            a[tpcent] = pval
            
        elif m in [mfwci, mscholar_output, mcit, mcit_pubs, mcit_per_pub]:
            
            val = row[cval]
            
            t = "{}_{}".format(m, y)
            
            a[t] = val

    return a


if __name__ == "__main__":

    # where all data is stored
    dname = os.path.join(BASE_DIR, "data", "metric_response")

    drop_cols = ['uri', 'type', 'link']
    excel_fname = "all_affiliations.xlsx"

    all_fnames = glob.glob(os.path.join(dname, "*.csv"))

    df_list = [pd.DataFrame(transform_pd(pd.read_csv(x))) for x in all_fnames]
    df = pd.concat(aaa)
    df.set_index('name')
    df.to_excel(excel_fname)

