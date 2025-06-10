#!/usr/bin/python2.7
#
# Interface for the assignement
#

import os
import psycopg2
import concurrent.futures
import functools
import time

import io
import queue
import string
import threading


DATABASE_NAME = 'dds_assgn1'


def create_partition_process(partition_index, ratingstablename, numberofpartitions):
        delta = 5 / numberofpartitions
        minRange = partition_index * delta
        maxRange = minRange + delta
        table_name = f"range_part{partition_index}"
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
    con = openconnection
    cur = con.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {ratingstablename};")

    cur.execute(f"""
        CREATE UNLOGGED TABLE {ratingstablename} (
            userid INTEGER,
            movieid INTEGER,
            rating FLOAT
        );
    """)
    con.commit()

    temp_file = ratingsfilepath + ".clean"
    rows = [0]
    with open(ratingsfilepath, 'r') as infile, open(temp_file, 'w') as outfile:
        for line in infile:
            parts = line.strip().split(':')
            if len(parts) >= 5:
                outfile.write(f"{parts[0]}\t{parts[2]}\t{parts[4]}\n")
            rows[0]+=1
    # Load into DB
    with open(temp_file, 'r') as f:
        cur.copy_from(f, ratingstablename, sep='\t', columns=('userid', 'movieid', 'rating'))
    end_time = time.time()
    con.commit()

    print("Thời gian thực thi loadratings: {:.2f} giây".format(end_time - start_time))

    # add logged to table for rollback if crash
    cur.execute(f"ALTER TABLE {ratingstablename} SET LOGGED;")

    # prepare for roundrobin insert

    cur.execute("create table info (tablename varchar, numrow integer);")
    cur.execute(f"insert into info (tablename, numrow) values ('{ratingstablename}', {rows[0]});")

    # Trigger cho INSERT
    cur.execute(f'''
        CREATE OR REPLACE FUNCTION update_row_count_func()
        RETURNS TRIGGER AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                UPDATE info SET numrow = numrow + 1 WHERE tablename = '{ratingstablename}';
            ELSIF TG_OP = 'DELETE' THEN
                UPDATE info SET numrow = numrow - 1 WHERE tablename = '{ratingstablename}';
            END IF;
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
# round robin theo đơn luồng
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()

    RROBIN_TABLE_PREFIX = 'rrobin_part'
    # tạo bảng tạm
    TEMP_TABLE = 'temp'
    cur.execute(f'''
        CREATE UNLOGGED TABLE {TEMP_TABLE} AS 
        SELECT *, ROW_NUMBER() OVER() - 1 AS stt FROM {ratingstablename};
    ''')
    # thực hiện các truy vấn tạo phân mảnh.
    for i in range(numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute(f'''
            CREATE TABLE {table_name} AS
            SELECT userid, movieid, rating FROM {TEMP_TABLE}
            WHERE MOD(stt, {numberofpartitions}) = {i};
        ''')
    # xóa bảng tạm
    cur.execute(f'drop table {TEMP_TABLE};')
    con.commit()
    cur.close()
## round robin partition đa luồng
# đoạn mã sử dụng 2 luồng để thực thi truy vấn
# hàm nhận và thực thi câu lệnh sql từ hàng đợi
# def rrobin_execute_query(sql_list: queue):
#     con = getopenconnection(dbname=DATABASE_NAME)
#     while True:
#         sql = sql_list.get() # lấy lệnh sql từ hàng đợi
#         """
#         Nếu lấy được giá trị None (tín hiệu báo hết truy vấn) từ hàng đợi thì thoát khỏi vòng lặp, đóng kết nối.
#         """
#         if sql is None:
#             break
#         cur = con.cursor()
#         cur.execute(sql)
#         con.commit()
#         cur.close()
#     con.close()
# def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
#     con = openconnection
#     cur = con.cursor()
#     RROBIN_TABLE_PREFIX = 'rrobin_part'
#     TEMP_TABLE = 'temp_ratings'
#     NUM_THREADS = 2
#     cur.execute(f'''
#         CREATE UNLOGGED TABLE {TEMP_TABLE} AS
#         SELECT *, ROW_NUMBER() OVER() - 1 AS stt FROM {ratingstablename};
#     ''')
#     con.commit()
#     sql_list = queue.Queue()
#     # tạo các câu truy vấn và đẩy vào hàng đợi.
#     for i in range(numberofpartitions):
#         table_name = RROBIN_TABLE_PREFIX + str(i)
#         sql= f'''
#         CREATE TABLE {table_name} AS
#         SELECT userid, movieid, rating FROM {TEMP_TABLE}
#         WHERE MOD(stt, {numberofpartitions}) = {i};
#         '''
#         sql_list.put(sql)
#     # đặt None là tín hiệu hết truy vấn.
#     for i in range(NUM_THREADS):
#         sql_list.put(None)
#     threads =  []
#     for t in range(NUM_THREADS):
#         # tạo luồng để thực thi hàm rrobin_execute_query và truyền vào hàng đợi chứa lệnh sql
#         t = threading.Thread(target=rrobin_execute_query, args=(sql_list,))
#         t.start() # thực thi luồng
#         threads.append(t)
#     # chờ các luồng thực hiện xong
#     for t in threads:
#         t.join()
#     cur.execute(f'drop table {TEMP_TABLE};') # xóa bảng tạm
#     con.commit()
#     cur.close()

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

