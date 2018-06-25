import pymongo
import sys
import subprocess
from pymongo import MongoClient
import pandas as pd


# if __name__ == "__main__":
def dump():

    # dump data
    _, _, db_name, coll_names, out = sys.argv

    print('db_name {}\tcoll_names {}\t'.format(db_name, coll_names))

    [print(x) for x in coll_names.split(',')]
    colls = coll_names.split(',')

    command = 'mongodump --db {} --collection {} --out {}'

    a = [command.format(db_name, x, out) for x in colls]
    print(a)

    [subprocess.call(x.split(' ')) for x in a]




def json_export():

    # dump data
    _, _, db_name, coll_names, out = sys.argv

    print('db_name {}\tcoll_names {}\t'.format(db_name, coll_names))

    [print(x) for x in coll_names.split(',')]
    colls = coll_names.split(',')

    command = 'mongoexport --db {} --collection {} --out {}'

    a = [command.format(db_name, x, out) for x in colls]
    print(a)

    [subprocess.call(x.split(' ')) for x in a]


def csv_export():

    _, _, db_name, coll_names, out = sys.argv

    print('db_name {}\tcoll_names {}\t'.format(db_name, coll_names))

    [print(x) for x in coll_names.split(',')]
    colls = coll_names.split(',')

    command = 'mongoexport --db {} --collection {} --out {}'

    a = [command.format(db_name, x, out) for x in colls]
    print(a)

    [subprocess.call(x.split(' ')) for x in a]


def restore():

    # mongorestore -d some_other_db -c some_or_other_collection dump/some_collection.bson

    _, _, db_name, coll_names, file_names = sys.argv

    print('db_name {}\tcoll_names {}\t'.format(db_name, coll_names))

    [print(x) for x in coll_names.split(',')]
    colls = coll_names.split(',')
    fnames = file_names.split(',')

    print(colls)
    print(len(colls))

    print(fnames)
    print(len(fnames))

    command = 'mongorestore --db {} --collection {} {}'

    assert len(colls) == len(fnames), 'lengths of coll_names and file_names are not the same'

    a = [command.format(db_name, colls[i], fnames[i]) for i, x in enumerate(colls)]
    print(a)

    [subprocess.call(x.split(' ')) for x in a]



# if __name__ == "__main__":
# def main():



    cmd = sys.argv[1]

    print(cmd)


    if cmd == 'dump':
        print('doing dump')
        dump()
    elif cmd == 'restore':
        restore()



#def concat():
if __name__ == "__main__":

    db_name_ids = 'temp'
    db_name_metrics = 'temp'

    coll_name_ids = 'name_ids'
    coll_name_metrics = 'metrics_apr_19'

    address = 'localhost'
    port = 27017

    client = MongoClient(address, port)

    db_ids = client[db_name_ids]
    db_metrics = client[db_name_metrics]


    coll_ids = db_ids[coll_name_ids]
    coll_metrics = db_metrics[coll_name_metrics]

    coll_name_temp = 'temp_coll'

    field_name_child_count = 'child_count'

    # variables
    scival_id = 'scival_id'
    aff = 'affiliation'

    query = [

        {
            '$match': {
                'scival_id': {'$exists': True}
            }
        },

        {
            '$addFields': {
                field_name_child_count: {
                    '$size': '$child_id'
                }
            }

        },

        {
            '$project': {
                '_id': 0,
                'scival_id': 1,
                'name': 1,
                field_name_child_count: 1
            }
        },
        {
            '$out': coll_name_temp
        }
    ]

    aggregated = list(coll_ids.aggregate(query))

    cname_year = 'year'
    cname_metric = 'metricType'
    cname_name = 'name'
    cname_res = 'results'
    cname_val = 'value'

    fname_result = 'long_book_patent.xlsx'

    years = [2014, 2015, 2016]

    # df = pd.DataFrame(columns=[cname_name, scival_id, cname_year, cname_metric])
    all_q = []



    for metrics in coll_metrics.find():

        cur_ids = coll_ids.find(
            {
                scival_id: metrics[aff][scival_id],
            },
        )

        a = list(cur_ids)

        q = {cname_name: a[0][cname_name],
             scival_id : a[0][scival_id],
             field_name_child_count: len(a[0]['child_id']),
             cname_metric: metrics[cname_res][cname_metric]}

        for k, v in metrics[cname_res][cname_val].items():

            qq = q.copy()
            qq.update({cname_year: int(k),
                       cname_val : v})

            all_q.append(qq)


    df = pd.DataFrame(all_q)
    df.to_excel(fname_result, index=False)
