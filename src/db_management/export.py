import pymongo
import sys
import subprocess


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

    _, _, db_name, coll_names, fnames = sys.argv

    print('db_name {}\tcoll_names {}\t'.format(db_name, coll_names))

    [print(x) for x in coll_names.split(',')]
    colls = coll_names.split(',')

    command = 'mongoexport --db {} --collection {} --out {}'

    assert len(colls_names) == len(fnames)

    a = [command.format(db_name, colls[i], fnames[i]) for i, x in enumerate(colls)]
    print(a)

    [subprocess.call(x.split(' ')) for x in a]



if __name__ == "__main__":
# def main():



    cmd = sys.argv[1]

    print(cmd)


    if cmd == 'dump':
        print('doing dump')
        dump()
    elif cmd == 'restore':
        restore()

