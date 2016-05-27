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
    p = argparse.ArgumentParser(description="Check and repair MongoDB fragmentation.")

    p.add_argument('-H', '--host', action='store', dest='host', default='127.0.0.1', help='The hostname you want to connect to')
    p.add_argument('-P', '--port', action='store', type=int, dest='port', default=27017, help='The port MongoDB is running on')
    p.add_argument('-u', '--user', action='store', dest='user', default=None, help='The username you want to login as')
    p.add_argument('-p', '--pass', action='store', dest='passwd', default=None, help='The password you want to use for that user')
    p.add_argument('-d', '--database', action='store', dest='database', default=None, help='Specify the database to check')
    p.add_argument('-s', '--summarize', action='store_true', dest='summarize', help='Summarize all fragmentation')
    p.add_argument('-r', '--replicaset', dest='replicaset', default=None, help='Connect to replicaset')
    p.add_argument('-A', '--action', dest='action', default='show', help='You can choice: show or compact.')

    options = p.parse_args()

    host = options.host
    port = options.port
    user = options.user
    passwd = options.passwd
    summarize = options.summarize
    database = options.database
    replicaset = options.replicaset
    action = options.action

    err, con = mongo_connect(host, port, user, passwd, replicaset)

    if err:
        return err

    if summarize:
        if database or action == 'compact':
            return 'Invalid usase. If you can summerize, only show the summarize of all database desfragmentation.'
        else:
            show_desfragmentation(con, True)
            sys.exit(1)

    if database:
        if action == 'compact':
            print ('Database %s' % database)
            compact_database(con, database)
        else:
            fragmentation = get_fragmentation(con, database)
            print ('%s = %s' %(database, (sizeof_fmt(fragmentation))))
    else:
        # Compact all databases
        data = get_info_dbs(con)

        for db in data['databases']:
            dbname = str(db['name'])
            if action == 'compact':
                print ('Database %s' % dbname)
                compact_database(con, dbname)
            else:
                fragmentation = get_fragmentation(con, dbname)
                print ('%s = %s' %(dbname, (sizeof_fmt(fragmentation))))

def show_desfragmentation(con, summarize=False):
    ''' Summarize all server desfragmentation '''
    data = get_info_dbs(con)

    size = 0
    for db in data['databases']:
        dbname = str(db['name'])
        fragmentation = get_fragmentation(con, dbname)
        if not summarize:
            print ('%s = %s' %(dbname, (sizeof_fmt(fragmentation))))
        else:
            size += fragmentation

    if summarize:
        print ('Total fragmentation = %s' %(sizeof_fmt(size)))

def get_fragmentation(con, dbname):
    ''' Get fragmentation of one database.'''
    stats = get_stats_db(con, dbname)

    if stats['dataSize'] <= stats['storageSize']:
        return stats['storageSize'] - stats['dataSize']
    else:
        return 0

def sizeof_fmt(num, suffix='B'):
    ''' Human readable size'''
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

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

# Compact database.
def compact_database(con, database):
    try:
        olddata = get_stats_db(con, database)

        collections = get_db_collections(con, database)

        for collection in collections:
            if collection.find('system') == -1:
                print('Compact %s' % collection)
                con[database].command(son.SON([('compact', collection)]))

        newdata = get_stats_db(con, database)

        print ( u"Srhink: %.0f MB" % ( olddata['storageSize'] - newdata['storageSize'] ))

        return True

    except Exception, e:
        return exit_with_general_critical(e)

def get_db_collections(con, database):
    try:
        dbase = con[database]
        return dbase.collection_names()

    except Exception, e:
        return exit_with_general_critical(e)

def get_info_dbs(con):
    try:
        data = con.admin.command(pymongo.son_manipulator.SON([('listDatabases', 1)]))
    except:
        data = con.admin.command(son.SON([('listDatabases', 1)]))
    return data

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))