import patent_count.get_patent_count_via_mongo as patent_count
import sys
import time


if __name__ == "__main__":

    _, year, n, rep = sys.argv

    year = int(year)
    n = int(n)
    rep = int(rep)

    metricType = 'PatentCount'
    queryTypes = ["by_id", "by_name"]
    queryType = queryTypes[1]

    db_name = 'patent_count_may17'


    ack_params = {'db_name': db_name,
                  'coll_name': 'acks_{}'.format(queryType),
                  'query_type': queryType}


    metrics_params = {'db_name': db_name,
                      'coll_name': 'metrics_{}'.format(queryType),
                      'query_type': queryType}


    for i in range(rep):

        print('year {}\nn is {}\rep is {}'.format(year, n, rep))

        driver = patent_count.main(n=n, year=year, metricType=metricType, ack_params=ack_params, metrics_params=metrics_params)
        time.sleep(5)
