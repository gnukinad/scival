import patent_count.get_patent_count_via_mongo as patent_count
import sys
import time


if __name__ == "__main__":

    _, year, n, rep = sys.argv

    year = int(year)
    n = int(n)
    rep = int(rep)

    metricType = 'PatentCount'

    ack_params = {'db_name': 'acknowledgments',
                  'coll_name': 'acks_apr19'}

    metrics_params = {'db_name': 'scopus_metrics',
                      'coll_name': 'metrics_apr19'}

    for i in range(rep):

        print('year {}\nn is {}\rep is {}'.format(year, n, rep))

        driver = patent_count.main(n=n, year=year, metricType=metricType, ack_params=ack_params, metrics_params=metrics_params)
        time.sleep(5)
