# -*- coding: utf-8 -*-
#!/usr/bin/env python

from __future__ import print_function
import sys
import argparse

try:
    import pymongo
except ImportError, e:
    print (e)
    sys.exit(2)

# As of pymongo v 1.9 the SON API is part of the BSON package, therefore attempt
# to import from there and fall back to pymongo in cases of older pymongo.
if pymongo.version >= "1.9":
    import bson.son as son
else:
    import pymongo.son as son

def main(argv):
    p = argparse.ArgumentParser(description="Search and remove all empty collections without indexs")

    p.add_argument('-H', '--host', action='store', dest='host', default='127.0.0.1', help='The hostname you want to connect to')
    p.add_argument('-P', '--port', action='store', type=int, dest='port', default=27017, help='The port MongoDB is running on')
    p.add_argument('-u', '--user', action='store', dest='user', default=None, help='The username you want to login as')
    p.add_argument('-p', '--pass', action='store', dest='passwd', default=None, help='The password you want to use for that user')
    p.add_argument('-d', '--database', action='store', dest='database', default=None, help='Specify the database to check')
    p.add_argument('-s', '--summarize', action='store_true', dest='summarize', help='Summarize all empty collections without indexes')
    p.add_argument('-r', '--replicaset', dest='replicaset', default=None, help='Connect to replicaset')
    p.add_argument('-D', '--drop', action='store_true', dest='drop', help='Drop all empty collections or databases.')

    options = p.parse_args()

    host = options.host
    port = options.port
    user = options.user
    passwd = options.passwd
    summarize = options.summarize
    database = options.database
    replicaset = options.replicaset
    drop = options.drop

    err, con = mongo_connect(host, port, user, passwd, replicaset)

    if err:
        return err

    #if summarize:
    #    if database or action == 'remove':
    #        return 'Invalid usase. If you can summerize, only show the summarize of all empty database collection.'
    #    else:
    #        num = count(con)
    #        print('%s empty collections of all databases' % num )
    #        sys.exit(1)

    if database:
        ncol = count(con, database)
        emptycols = empty_collections(con, database, drop)
        if not drop:
            print ('database = empty collections / total database collections')
            print ('%s = %s / %s' %(database, emptycols, ncol))
    else:
        # Get all databases
        databases = get_info_dbs(con)
        if len(databases) > 0:
            print ('database = empty collections / total database collections')

        for db in databases:
            ncol = count(con, db)
            emptycols = empty_collections(con, db, drop)
            if not drop:
                print ('%s = %s / %s' %(db, emptycols, ncol))

def count(con, database=None):
    ''' Count collections '''
    collections = get_db_collections(con, database)
    return len(collections)

def empty_collections(con, database, drop=False):
    ''' Search and find emtpy collections '''
    collections = get_db_collections(con, database)

    ncol = 0
    for c in collections:
        # Emtpy collection?
        if not c.endswith('.'):
            if con[database][c].count() == 0:
                # Indexs?
                indexs = con[database][c].index_information()
                # Ensure it have not indexes
                if len(indexs) <= 1:
                    if drop:
                        print ('Drop "%s" collection' % c)
                        con[database][c].drop()
                    ncol += 1

    newlen = get_db_collections(con, database)
    if len(newlen) == 0 and drop:
        print ('Drop "%s" database because it have not collections' % database)
        con.drop_database(database)

    return ncol

def mongo_connect(host=None, port=None, user=None, passwd=None, replica=None):
    ''' MongoDB connect '''
    try:
        if pymongo.version >= "2.3":
            if replica is None:
                con = pymongo.MongoClient(host, port)
            else:
                con = pymongo.MongoClient(host, port, read_preference=pymongo.ReadPreference.SECONDARY, replicaSet=replica)
        else:
            if replica is None:
                con = pymongo.Connection(host, port, slave_okay=True, network_timeout=3)
            else:
                con = pymongo.Connection(host, port, slave_okay=True, replicaSet=replica, network_timeout=3)

        if user and passwd:
            db = con["admin"]
            if not db.authenticate(user, passwd):
                sys.exit("Username/Password incorrect")
    except Exception, e:
        if isinstance(e, pymongo.errors.AutoReconnect) and str(e).find(" is an arbiter") != -1:
            # We got a pymongo AutoReconnect exception that tells us we connected to an Arbiter Server
            # This means: Arbiter is reachable and can answer requests/votes - this is all we need to know from an arbiter
            print ("State: 7 (Arbiter)")
            sys.exit(0)
        return exit_with_general_critical(e), None
    return 0, con


def exit_with_general_critical(e):
    if isinstance(e, SystemExit):
        return e
    else:
        print ("General MongoDB Error:", e)
    return 2


def get_stats_db(con, database):
    return con[database].command('dbstats')

def get_db_collections(con, database):
    try:
        return con[database].collection_names(False)

    except Exception, e:
        return exit_with_general_critical(e)

def get_info_dbs(con):
    ''' List all databases '''
    try:
        data = con.database_names()
    except Exception, e:
        return exit_with_general_critical(e)

    # Drop a system database.
    if data:
        if 'system' in data:
            data.remove('system')

    return data

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))