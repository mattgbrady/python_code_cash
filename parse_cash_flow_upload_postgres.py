import pandas as pd
from pandas import DataFrame as df
import psycopg2
import numpy as np
import time
from datetime import datetime
from datetime import date
from datetime import timedelta



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
        print data_list
        #function call
        data_list_2 = tuple(data_list)

        conn.execute('INSERT INTO '+table_name+ ' ('+ column_string +') VALUES (' + input_string + ');', (data_list_2))
        conn.connection.commit() 

def delete_duplicate_created_at(current_date,db_name,columns,temp_df,conn):
    conn.execute('DELETE FROM '+ db_name +' WHERE created_at = '+current_date+'')
    conn.connection.commit() 
    return

def process_file(conn,path_2,db_name):

    temp_df = pd.read_csv(path_2)
    temp_df = temp_df.set_index(temp_df['the_date'])

    temp_df = temp_df.drop(temp_df.columns[0], axis=1)

    columns = temp_df.columns.tolist()
    num_columns = str(len(columns))
    current_date = datetime.now().strftime("%Y-%m-%d")
    current_date = "'"+current_date+"'"
    #function call
    delete_duplicate_created_at(current_date,db_name,columns,temp_df,conn)
    upload_to_db(db_name,columns,temp_df,conn)   

def format_df(df):
    df = df.sort('Cash Flow Name', na_position='first')
    df = df[6:]
    column = df['Account Name']
    column = [x for x in column]
    new_index = range(0,len(df.index))
    df = df.sort('Account Name')
    df['new_index'] = new_index
    df.fillna(value=0, inplace=True)
    return df.set_index('new_index')

def scrub_loop_df(df,index_value):
    temp_series = df.ix[index_value]
    temp_series = temp_series[4:]
    temp_series = temp_series.dropna()
    return temp_series
        
def main():

    start_time = time.time()

    path_1 = 'X:\Users Seattle\Solutions\Clients\OPG\Cash Management\Projected Cash Flows/2015\OPG Cash Flow 2015.xlsx'

    temp_df = pd.read_excel(path_1, 'Operating Cash')

    formatted_df = format_df(temp_df)

    previous_index = formatted_df.index.values

    new_df = df(columns=['account_name', 'cash_flow_name','cash_flow_type','cash_flow_currency','cash_flow_amount'])

    for counter in range(0,len(formatted_df)):

        index_value = previous_index[counter]
        account_name = formatted_df.loc[index_value,'Account Name']
        cash_flow_name = formatted_df.loc[index_value,'Cash Flow Name']
        cash_flow_type = formatted_df.loc[index_value,'Cash Flow Type']
        cash_flow_currency = formatted_df.loc[index_value,'Currency']

        temp_series = scrub_loop_df(formatted_df,index_value)
        date_list = temp_series.index.tolist()
    
        cash_flow_amount = temp_series.values

        num_rows = temp_series.count()

        account_name_array = [account_name] * num_rows
        cash_flow_name_array = [cash_flow_name] * num_rows
        cash_flow_type_array = [cash_flow_type] * num_rows
        cash_flow_currency_array = [cash_flow_currency] * num_rows
        temp_dict = {}

        temp_dict['account_name'] = account_name_array
        temp_dict['cash_flow_name'] = cash_flow_name_array
        temp_dict['cash_flow_type'] = cash_flow_type
        temp_dict['cash_flow_currency'] = cash_flow_currency
        temp_dict['cash_flow_amount'] = cash_flow_amount

        loop_df = df(temp_dict, index=date_list)
        column_order = ['account_name', 'cash_flow_name','cash_flow_type','cash_flow_currency','cash_flow_amount']
        loop_df = loop_df[column_order]
        new_df = new_df.append(loop_df)
        new_df.index.name = 'the_date'
    


    current_date = datetime.today()
    
    current_date = current_date.now().date()
  
    end_date = current_date + timedelta(days=90)

 
    new_df = new_df.reset_index()
    
    new_df = new_df[(new_df['the_date'] >= current_date) & (new_df['the_date'] <= end_date)]

    new_df.to_csv('formatted_cash_flows.csv')

    #need to define these variables
    host_name = 'host_name'
    port = 'port'
    username = 'username'
    password = 'password'
    database = 'database'

    host_name = 'v-devsvrse01.corp.wurts.com'
    port = '5432'
    username = 'mbrady'
    password = 'ulnae1,brood'
    database = 'aquarium'

    conn = connect_to_database(host_name,port,username,password,database)

    db_name = 'the_zoo.sti_cash_flows'

    path_2 = 'formatted_cash_flows.csv'

    process_file(conn,path_2,db_name)

    end_time = time.time()
    time_elapsed = int(end_time - start_time)
    minutes, seconds = time_elapsed // 60, time_elapsed % 60

    print("Processing time is " + str(minutes) + ":" + str(seconds).zfill(2))

main()
