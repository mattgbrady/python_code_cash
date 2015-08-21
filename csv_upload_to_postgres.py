import pandas as pd
from pandas import DataFrame as df
import psycopg2
import numpy as np
import time
from datetime import datetime
from datetime import date
from datetime import timedelta
from user_credentials import database_credentials

def create_csv(path,worksheet_name_dict):

    for worksheet_name, table_name in worksheet_name_dict.iteritems():
        temp_df = pd.read_excel(path,worksheet_name)
        temp_df.to_csv(worksheet_name + ".csv", index=False)

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

def replace_nan(data_list):
    for each in range(0,len(data_list)):
        if str(data_list[each]) == 'nan':
            data_list[each] = None       
    return data_list

def delete_duplicate_created_at(current_date,db_name,columns,temp_df,conn):
    conn.execute('DELETE FROM '+ db_name +' WHERE created_at = '+current_date+'')
    conn.connection.commit() 
    return

def upload_to_db(table_name,columns,temp_df,conn):

    #function call
    column_string = column_str(columns)
    #function call
    input_string = input_str(len(columns))

    for row in temp_df.iterrows():

        index, data = row
        data_list = data.tolist()

        #function call
        data_list_2 = replace_nan(data_list)

        data_list_3 = tuple(data_list_2)
        conn.execute('INSERT INTO ' + table_name + ' ('+ column_string +') VALUES (' + input_string + ');', (data_list_3))
        conn.connection.commit() 

def process_file(conn,path,worksheet_name_dict):

    #function call
    create_csv(path,worksheet_name_dict)
    current_date = datetime.now().strftime("%Y-%m-%d")
    current_date = "'"+current_date+"'"

    for worksheet_name, table_name in worksheet_name_dict.iteritems():
        print table_name
        temp_df = pd.read_csv(worksheet_name + ".csv")
        columns = temp_df.columns.tolist()
        num_columns = str(len(columns))
        
        #function call
        delete_duplicate_created_at(current_date,table_name,columns,temp_df,conn)
        upload_to_db(table_name,columns,temp_df,conn)   
        
def main():

    start_time = time.time()

    # from user_credentials file
    host_name = database_credentials.host_name 
    port = database_credentials.port
    username = database_credentials.username
    password = database_credentials.password
    database = database_credentials.database

    #data in worksheet needs to be in header(column) and row structure
    worksheet_name_dict = {'Issuers': 'the_zoo.sti_issuers' ,'Ticker Information': 'the_zoo.sti_ticker_information',
                        'Market Value': 'the_zoo.sti_daily_mv','Rating': 'the_zoo.sti_ratings', 'Rating Scale': 'the_zoo.rating_scale', 
                        'Money Market Yield': 'the_zoo.sti_money_mkt_yield' }

    #function call
    conn = connect_to_database(host_name,port,username,password,database)

    #function call
    path = 'X:\Users Seattle\Solutions\Clients\OPG\Cash Management\Portfolio Reports\portfolio_tables.xlsx'
    temp_df = process_file(conn,path,worksheet_name_dict)

    end_time = time.time()
    time_elapsed = int(end_time - start_time)
    minutes, seconds = time_elapsed // 60, time_elapsed % 60

    print("Processing time is " + str(minutes) + ":" + str(seconds).zfill(2))

main()


