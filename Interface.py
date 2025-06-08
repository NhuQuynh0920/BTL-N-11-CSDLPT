#!/usr/bin/python2.7
#
# Interface for the assignement
#

from dotenv import load_dotenv
import os
import psycopg2
import concurrent.futures
import functools
import time

load_dotenv()

DATABASE_NAME = os.getenv("DB_NAME")
NUMBER_TABLE = int(os.getenv("NUMBER_TABLE"))

def create_partition_process(partition_index, ratingstablename, numberofpartitions):
        delta = NUMBER_TABLE / numberofpartitions
        minRange = partition_index * delta
        maxRange = minRange + delta
        table_name = f"range_part{partition_index}"
        print(table_name)
        con = psycopg2.connect(database='dds_assgn1', user='postgres', password='1234', host='localhost', port='5432')
        cur = con.cursor()

        cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT);")

        if partition_index == 0:
            cur.execute(f"""
                INSERT INTO {table_name}
                SELECT userid, movieid, rating FROM {ratingstablename}
                WHERE rating >= %s AND rating <= %s;
            """, (minRange, maxRange))
        else:
            cur.execute(f"""
                INSERT INTO {table_name}
                SELECT userid, movieid, rating FROM {ratingstablename}
                WHERE rating > %s AND rating <= %s;
            """, (minRange, maxRange))

        con.commit()
        cur.close()
        con.close()


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection): 
    start_time = time.time()
    
    create_db(DATABASE_NAME)
    con = openconnection
    cur = con.cursor()
    cur.execute("create table " + ratingstablename + "(userid integer, extra1 char, movieid integer, extra2 char, rating float, extra3 char, timestamp bigint);")
    cur.copy_from(open(ratingsfilepath),ratingstablename,sep=':')
    cur.execute("alter table " + ratingstablename + " drop column extra1, drop column extra2, drop column extra3, drop column timestamp;")
    cur.close()
    con.commit()
    end_time = time.time()
    print("Thời gian thực thi loadratings: {:.2f} giây".format(end_time - start_time))

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    start_time = time.time()

    func = functools.partial(create_partition_process, ratingstablename=ratingstablename, numberofpartitions=numberofpartitions)
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(func, range(numberofpartitions))

    end_time = time.time()
    print("Thời gian thực thi rangepartition : {:.2f} giây".format(end_time - start_time))

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    cur.execute("insert into " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.execute("select count(*) from " + ratingstablename + ";");
    total_rows = (cur.fetchall())[0][0]
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    index = (total_rows-1) % numberofpartitions
    table_name = RROBIN_TABLE_PREFIX + str(index)
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.close()
    con.commit()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    delta = 5 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index = index - 1
    table_name = RANGE_TABLE_PREFIX + str(index)
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.close()
    con.commit()

def create_db(dbname):

    """
    We create a DB by connecting to the default user and database of Postgres

    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def count_partitions(prefix, openconnection):
    """

    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()

    return count
