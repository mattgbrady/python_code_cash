import pandas as pd
from pandas import DataFrame as df
import psycopg2
import numpy as np
import time
from datetime import date
from datetime import datetime
from datetime import timedelta
import sys
from user_credentials import database_credentials

def connect_to_database(host_name,port,username,password,database):

    try:
        conn = psycopg2.connect(database=database, user=username, password=password, host=host_name,port=port)
        cur = conn.cursor()
        return cur
    except:
        print "I am unable to connect to the database"  

def column_str(columns):

    column_string = ""
    for count in range(0,len(columns)):
        column_string+=columns[count]+','
    return column_string[:-1]

def input_str(len_columns):
    input_string = ""
    for each in range(len_columns):
        input_string+='%s'+','
    return input_string[:-1]

def get_column_names(conn,table_name):

    sql =  ("SELECT * FROM %s " % (table_name))
    conn.execute(sql)

    columns = []

    for count in range(0,len(conn.description)):
        columns.append(conn.description[count][0])

    columns = columns[1:-1]
    return columns

def upload_to_db(conn,temp_df,table_name):
    
    #date_look_back
    days_look_back = 45

    min_date = min(temp_df['the_date'])
    max_date = max(temp_df['the_date'])

    date_look_back =max(temp_df['the_date'])-timedelta(days=days_look_back)

    if min_date <= date_look_back:
        temp_df = temp_df[temp_df['the_date'] >=  date_look_back]

    temp_df.sort('the_date', inplace=True)
    sql = ('TRUNCATE ' + table_name)

    db_name = table_name

    conn.execute(sql)
    conn.connection.commit()

    columns = get_column_names(conn,table_name)

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

def created_df_from_postgres(conn,db_name):

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

def get_positions_percentage(temp_df, index):

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

def join_type_view_df(daily_mv_df,ticker_information_df,issuer_df):

    ticker_temp_df = ticker_information_df[['security_bloomberg_ticker','issuer_name','annualized_yield','maturity_date','category']]
    check_security_duplicates(ticker_temp_df)

    ticker_temp_df.drop('security_bloomberg_ticker_dup', axis=1, inplace=True)
 
    daily_mv_df.reset_index(inplace=True)

    ticker_temp_df.set_index('security_bloomberg_ticker', inplace=True)

    daily_mv_df.join(ticker_temp_df, on ='security_bloomberg_ticker')

    daily_mv_df = daily_mv_df.join(ticker_temp_df, on ='security_bloomberg_ticker')

    issuer_df.set_index('issuer_name',inplace=True)

    daily_mv_df = daily_mv_df.join(issuer_df, on ='issuer_name')

    return daily_mv_df

def type_view(conn,daily_mv_df,table_name,group_type):

    grouped = daily_mv_df.groupby(by=['the_date',group_type]).sum()

    temp_df = df(grouped)

    temp_df.reset_index(inplace=True)

    temp_df = temp_df[['the_date',group_type,'percentage']]

    upload_to_db(conn,temp_df,table_name)

def maturity_bucket(row):
    
    if row.number_of_days == 1:
        return 'Overnight'
    elif row.number_of_days > 1 and row.number_of_days <= 5:
        return '2-5'
    elif row.number_of_days > 5 and row.number_of_days <= 10:
        return '6-10'   
    elif row.number_of_days > 10 and row.number_of_days <= 20:
        return '11-20'  
    elif row.number_of_days > 20 and row.number_of_days <= 30:
        return '21-30'  
    elif row.number_of_days > 30 and row.number_of_days <= 45:
        return '31-45'  
    elif row.number_of_days > 45 and row.number_of_days <= 60:
        return '46-60'  
    elif row.number_of_days > 60 and row.number_of_days <= 90:
        return '61-90'  
    elif row.number_of_days > 90:
        return '>91'  

def maturity_bucket_view(conn,daily_mv_df,ticker_information_df,table_name):

    #only to be used for testing this code
    #date should be current
    testing_date = min(daily_mv_df['the_date']).date()

    #ticker_information_df['number_of_days'] = daily_mv_df['the_date'] - testing_date

    daily_mv_df['number_of_days'] = daily_mv_df['maturity_date'] - testing_date


    daily_mv_df['number_of_days'] = daily_mv_df['number_of_days'].astype('timedelta64[D]').astype(int)

    daily_mv_df.number_of_days[daily_mv_df['security_bloomberg_ticker'] == 'Cash'] = 1

    daily_mv_df['maturity_bucket'] = None

    daily_mv_df['maturity_bucket'] = daily_mv_df.apply(maturity_bucket, axis=1)

    grouped = daily_mv_df.groupby(by=['the_date','maturity_bucket']).sum()

    temp_df = df(grouped)

    
    temp_df.reset_index(inplace=True)
    unique_date = set(temp_df['the_date'].values)
    unique_date = unique_date[x.date for x in unique_date]

    bucket_array = ['Overnight', '2-5', '6-10','11-20','21-30','31-45','46-60','61-90','>91']



    temp_df.reset_index(inplace=True)

    temp_df = temp_df[['the_date','maturity_bucket','market_value','percentage']]


    #upload_to_db(conn,temp_df,table_name)

def process_daily_data(conn):

    issuer_df = created_df_from_postgres(conn,db_name='the_zoo.sti_issuers')
    daily_mv_df = created_df_from_postgres(conn,db_name='the_zoo.sti_daily_mv')
    ticker_information_df = created_df_from_postgres(conn,db_name='the_zoo.sti_ticker_information')
    
    daily_mv_df = get_positions_percentage(daily_mv_df,index=['the_date','security_bloomberg_ticker'])

    daily_mv_df = join_type_view_df(daily_mv_df,ticker_information_df,issuer_df)

    daily_mv_df.drop('id', axis=1, inplace=True)

    return daily_mv_df, ticker_information_df  

def main():

    start_time = time.time()

    # from user_credentials file
    host_name = database_credentials.host_name 
    port = database_credentials.port
    username = database_credentials.username
    password = database_credentials.password
    database = database_credentials.database


    conn = connect_to_database(host_name,port,username,password,database)

    daily_mv_df, ticker_information_df = process_daily_data(conn)

    #type_view(conn,daily_mv_df, table_name = 'the_zoo.sti_daily_sector_view',group_type='security_sector')
    #type_view(conn,daily_mv_df, table_name = 'the_zoo.sti_daily_industry_view',group_type='industry')
    #type_view(conn,daily_mv_df, table_name = 'the_zoo.sti_daily_instrument_view',group_type='category')
    maturity_bucket_view(conn,daily_mv_df,ticker_information_df, table_name = 'the_zoo.sti_daily_maturity_bucket_view')


    end_time = time.time()
    time_elapsed = int(end_time - start_time)
    minutes, seconds = time_elapsed // 60, time_elapsed % 60

    print("Processing time is " + str(minutes) + ":" + str(seconds).zfill(2))

main()
