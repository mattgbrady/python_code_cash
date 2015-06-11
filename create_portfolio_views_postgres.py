import pandas as pd
from pandas import DataFrame as df
import psycopg2
import numpy as np
import time
from datetime import date
from datetime import datetime
from datetime import timedelta
import sys

def connect_to_database(host_name,port,username,password,database):

    try:
        conn = psycopg2.connect(database=database, user=username, password=password, host=host_name,port=port)
        cur = conn.cursor()
        return cur
    except:
        print "I am unable to connect to the database"  

def column_str(columns):
    column_string = ""
    for each in columns:
        column_string+=each+','
    return column_string[:-1]

def input_str(len_columns):
    input_string = ""
    for each in range(len_columns):
        input_string+='%s'+','
    return input_string[:-1]

def upload_to_db(table_name,columns,temp_df,conn):

    #function call
    column_string = column_str(columns)
    #function call
    input_string = input_str(len(columns))

    for row in temp_df.iterrows():

        index, data = row
        data_list = data.tolist()
      
        #function call
        data_list_2 = tuple(data_list)

        conn.execute('INSERT INTO '+table_name+ ' ('+ column_string +') VALUES (' + input_string + ');', (data_list_2))

    conn.connection.commit()

def created_df(conn,db_name):

    sql = ("SELECT max(created_at) FROM %s" % (db_name))

    conn.execute(sql)
    latest_created_at = conn.fetchone()
    latest_created_at = latest_created_at[0]
    latest_created_at = latest_created_at.strftime("%Y-%m-%d")
    latest_created_at = "'"+latest_created_at+"'"
    sql =  ("SELECT * FROM %s WHERE created_at = %s " % (db_name,latest_created_at))
    conn.execute(sql)
    date_tuple =  conn.fetchall()
    columns = []

    for count in range(0,len(conn.description)):
        columns.append(conn.description[count][0])

    temp_df = df.from_records(date_tuple, columns=columns)

    return temp_df

def format_issuer_df(temp_df, index):

    min_date = min(temp_df['the_date'])
    max_date = max(temp_df['the_date'])

    temp_df.set_index(index, inplace=True)

    temp_df.drop(['created_at','id'], axis=1, inplace=True)

    temp_df.sortlevel(0,ascending=True,inplace=True)

    grouped_sum = temp_df.groupby(level='the_date').sum()

    temp_df['percentage'] = temp_df.div(grouped_sum,axis=0)

    return temp_df

def check_security_duplicates(ticker_temp_df):
    ticker_temp_df['security_bloomberg_ticker_dup'] = ticker_temp_df.duplicated()
    duplicate_number = max(ticker_temp_df[ticker_temp_df.security_bloomberg_ticker_dup == True].count())
    if duplicate_number != 0:
        print ticker_temp_df[ticker_temp_df.security_bloomberg_ticker_dup == True]
        sys.exit("ERROR");




def categorize_issues(daily_mv_df,ticker_information_df,issuer_df):


    ticker_temp_df = ticker_information_df[['security_bloomberg_ticker','issuer_name']]
    check_security_duplicates(ticker_temp_df)

    ticker_temp_df.drop('security_bloomberg_ticker_dup', axis=1, inplace=True)
 

    daily_mv_df.reset_index(inplace=True)

    ticker_temp_df.set_index('security_bloomberg_ticker', inplace=True)

    daily_mv_df.join(ticker_temp_df, on ='security_bloomberg_ticker')

    daily_mv_df = daily_mv_df.join(ticker_temp_df, on ='security_bloomberg_ticker')

    issuer_df.set_index('issuer_name',inplace=True)

    daily_mv_df = daily_mv_df.join(issuer_df, on ='issuer_name')

    print daily_mv_df



def portfolio_views(conn):

    issuer_df = created_df(conn,db_name='the_zoo.sti_issuers')
    daily_mv_df = created_df(conn,db_name='the_zoo.sti_daily_mv')
    ticker_information_df = created_df(conn,db_name='the_zoo.sti_ticker_information')
    issuer_ratings_df = created_df(conn,db_name='the_zoo.sti_ratings')
    agency_rating_scale_df = created_df(conn,db_name='the_zoo.rating_scale')
    
    min_date = min(daily_mv_df['the_date'])
    max_date = max(daily_mv_df['the_date'])

    daily_mv_df = format_issuer_df(daily_mv_df,index=['the_date','security_bloomberg_ticker'])

    daily_mv_df = categorize_issues(daily_mv_df,ticker_information_df,issuer_df)

    

def main():

    start_time = time.time()

 	#need to define these variables
    host_name = 'host_name'
    port = 'port'
    username = 'username'
    password = 'password'
    database = 'database'
    path = 'path'


    conn = connect_to_database(host_name,port,username,password,database)

    portfolio_views(conn)

    end_time = time.time()
    time_elapsed = int(end_time - start_time)
    minutes, seconds = time_elapsed // 60, time_elapsed % 60

    print("Processing time is " + str(minutes) + ":" + str(seconds).zfill(2))



main()
