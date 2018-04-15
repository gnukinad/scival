
import pymongo
from model import scopus_metrics, metric_ack_model

from scopus_metrics import mongo_scopus_metrics
from metric_ack import mongo_metric_ack


# if __name__ == "__main__":
def test_patent_count():

    print('success')

    coll = mongo_scopus_metrics()

    item1 = {'scopus_id': 20111, 'metricType': 'BookCount', 'year': '2012', 'value': 22}
    item2 = {'scopus_id': 20111, 'metricType': 'PatentCount', 'year': '2012', 'value': 30}

    i1 = scopus_metrics(**item1)
    i1 = scopus_metrics(**item2)

    coll.update_item_by_year(**item1)
    coll.update_item_by_year(**item2)


# def test_acknowledgment_writing():
if __name__ == "__main__":

    print('test_acknoledgement_writing')

    coll = mongo_metric_ack()

    item1 = {'scopus_id': 60000650, 'metricType': 'BookCount', 'year': '2012', 'ack': 1}
    item2 = {'scopus_id': 60000650, 'metricType': 'PatentCount', 'year': '2012', 'ack': 1}
    item3 = {'scopus_id': 60000650, 'metricType': 'PatentCount', 'year': '2013', 'ack': 1}
    item4 = {'scopus_id': 60000650, 'metricType': 'PatentCount', 'year': '2014', 'ack': 1}
    item5 = {'scopus_id': 60000650, 'metricType': 'PatentCount', 'year': '2015', 'ack': 1}

    # i1 = scopus_metrics(**item1)
    # i1 = scopus_metrics(**item2)

    coll.update_item_by_year(**item1)
    coll.update_item_by_year(**item2)
    coll.update_item_by_year(**item3)
    coll.update_item_by_year(**item4)
    coll.update_item_by_year(**item5)


def test_acknowledgement():
# if __name__ == '__main__':

    print('test_acknoledgement')

    coll = mongo_metric_ack()

    q = coll.find_valid_ids('BookCount', '2012', 20)


    return q, coll
