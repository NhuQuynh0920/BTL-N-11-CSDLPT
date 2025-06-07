#!/usr/bin/python2.7
#
# Interface for the assignement
#
import io
import queue
import string
import threading
import time

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")
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
def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    """
    create_db(DATABASE_NAME)
    con = openconnection
    cur = con.cursor()
    start= time.time()
    q = queue.Queue(maxsize=20)
    cur.execute("create table " + ratingstablename + "(userid integer, movieid integer,  rating float);")
    con.commit()
    rows = [0]
    thread_read  = threading.Thread(target=reader, args=(q,ratingsfilepath,rows))

    thread_read.start()
    thread_w = []
    for i in range(4):
        t = threading.Thread(target=writer, args=(q,ratingstablename))
        t.start()
        thread_w.append(t)

    thread_read.join()
    for t in thread_w:
        t.join()
    con.commit()
    # cur.execute(f"alter table {ratingstablename} add  primary key (movieid, userid);")
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
    cur.close()
    con.commit()
    end= time.time()
    print("load ratings: " + str(end-start))

def rrobin_execute_query(sql_list):
    while True:
        sql = sql_list.get()
        if sql is None:
            break
        start = time.time()
        con = getopenconnection(dbname=DATABASE_NAME)
        cur = con.cursor()
        cur.execute(sql)
        con.commit()
        cur.close()
        con.close()
        end = time.time()
        print('sql', sql)
        print (f'runtime:  {end-start}')
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