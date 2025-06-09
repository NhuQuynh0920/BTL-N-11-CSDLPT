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

import io
import queue
import string
import threading

load_dotenv()

DATABASE_NAME = os.getenv("DB_NAME")
NUMBER_TABLE = int(os.getenv("NUMBER_TABLE"))

def clean_data(file_path:string):
    data= []
    with open (file_path,'r') as f:
        for line in f:
            try:
                val = line.split(':')
                result = f'{val[0]},{val[2]},{val[4]}'
                data.append(result)
            except:
                print('clean data error')
    return '\n'.join(data)
def reader(q:queue ,file_path:string, rows:list):
    data = []
    with open (file_path,'r') as f:
        for line in f:
            try:
                val = line.split(':')
                result = f'{val[0]},{val[2]},{val[4]}'
                data.append(result)
                if len(data) >=1000000:
                    rows[0] += len(data)
                    q.put('\n'.join(data))
                    data = []
            except:
                print('clean data error')
    rows[0] += len(data)
    q.put('\n'.join(data))

    for _ in range(4):
        q.put(None)
def writer(q:queue, table_name):
    id=0
    con = getopenconnection(dbname=DATABASE_NAME)
    while True:
        data = q.get()
        if data is None:
            break
        cur = con.cursor()
        start = time.time()
        input = io.StringIO(data)
        cur.copy_from(input, table_name, sep=',')
        cur.close()
        con.commit()
        end = time.time()
        id = id + 1
    con.close()

def create_partition_process(partition_index, ratingstablename, numberofpartitions):
        delta = 5 / numberofpartitions
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
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    """
    start_time = time.time()
    con = openconnection
    cur = con.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {ratingstablename};")
    #Create unlogged table
    cur.execute(f"""
        CREATE UNLOGGED TABLE {ratingstablename} (
            userid INTEGER,
            movieid INTEGER,
            rating FLOAT
        );
    """)
    con.commit()

    # Prehandling using awk
    temp_file = ratingsfilepath + ".clean"
    os.system(f"""awk -F: '{{print $1 "\\t" $3 "\\t" $5}}' "{ratingsfilepath}" > "{temp_file}" """)

    # Load into DB
    with open(temp_file, 'r') as f:
        cur.copy_from(f, ratingstablename, sep='\t', columns=('userid', 'movieid', 'rating'))
    
    #them phan cua Dai
    rows = [0]
    cur.execute("create table info (tablename varchar, numrow integer);")
    cur.execute(f"insert into info (tablename, numrow) values ('{ratingstablename}', {rows[0]});")
    # Trigger cho INSERT
    cur.execute(f'''
            CREATE OR REPLACE FUNCTION update_row_count_func()
            RETURNS TRIGGER AS $$
            BEGIN
                UPDATE info SET numrow = numrow + 1 WHERE tablename = '{ratingstablename}';
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
    ''')
    cur.execute(f"""
                    CREATE TRIGGER ratings_after_insert
                    AFTER INSERT ON {ratingstablename}
                    FOR EACH ROW
                    EXECUTE FUNCTION update_row_count_func();
                """)
    
    con.commit()
    cur.close()
    os.remove(temp_file)
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
    """
    Chạy phân mảnh theo range một cách song song (mỗi tiến trình có kết nối riêng).
    """
    start_time = time.time()

    func = functools.partial(create_partition_process, ratingstablename=ratingstablename, numberofpartitions=numberofpartitions)
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(func, range(numberofpartitions))

    end_time = time.time()
    print("Thời gian thực thi rangepartition : {:.2f} giây".format(end_time - start_time))
    
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm để tạo các partition của bảng chính bằng phương pháp round-robin sử dụng luồng.
    """
    con = openconnection
    cur = con.cursor()
    start = time.time()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    TEMP_TABLE = 'temp_ratings'
    cur.execute(
        f'CREATE UNLOGGED TABLE {TEMP_TABLE} AS SELECT *, ROW_NUMBER() OVER() - 1 AS stt FROM {ratingstablename};')
    con.commit()
    # đa luồng
    # sql_list = queue.Queue(maxsize=10)
    # for i in range(numberofpartitions):
    #     table_name = RROBIN_TABLE_PREFIX + str(i)
    #     sql= f'''
    #     CREATE TABLE {table_name} AS
    #     SELECT userid, movieid, rating FROM {TEMP_TABLE}
    #     WHERE MOD(stt, {numberofpartitions}) = {i};
    #     '''
    #     sql_list.put(sql)
    # for i in range(5):
    #     sql_list.put(None)
    # threads =  []
    # for t in range(5):
    #     t = threading.Thread(target=rrobin_execute_query, args=(sql_list,))
    #     t.start()
    #     threads.append(t)
    # for t in threads:
    #     t.join()
    # đơn luồng
    for i in range (numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute(f'''
            CREATE TABLE {table_name} AS
            SELECT userid, movieid, rating FROM {TEMP_TABLE}
            WHERE MOD(stt, {numberofpartitions}) = {i};
        ''')
    con.commit()
    cur.close()
    end = time.time()
    print("runtime: " + str(end - start) + " giây")


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
    """
    con = openconnection
    cur = con.cursor()
    start = time.time()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    cur.execute("insert into " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.execute(f"select numrow from info where tablename = '{ratingstablename}';")
    num_row = cur.fetchall()[0][0]
    num_partitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    id = (num_row-1) % num_partitions
    table_name = RROBIN_TABLE_PREFIX + str(id)
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.close()
    con.commit()
    end= time.time()
    print("round robin insert: " + str(end-start))
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
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()

    return count
