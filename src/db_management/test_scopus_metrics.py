
import pymongo
from model import scopus_metrics

from scopus_metrics import mongo_scopus_metrics


if __name__ == "__main__":

    print('success')


    coll = mongo_scopus_metrics()

    item1 = {'scopus_id': 20111, 'metricType': 'patent_count', 'year': '2012', 'value': 22}

    i1 = scopus_metrics(**item1)

    coll.update_item_by_year2(**item1)
