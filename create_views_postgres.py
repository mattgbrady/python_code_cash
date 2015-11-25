import pandas as pd
from pandas import DataFrame as df
import psycopg2
import numpy as np
import time
from datetime import date
from datetime import datetime
from datetime import timedelta
from user_credentials import database_credentials



def connect_to_database(host_name,port,username,password,database):

    try:
        conn = psycopg2.connect(database=database, user=username, password=password, host=host_name,port=port)
        cur = conn.cursor()
        return cur
    except:
        print("I am unable to connect to the database" ) 

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

def divide_1000(column):
    return column/1000


def sti_cf_view(conn,db_name):

    criteria_1 = 'STI CAD'
    criteria_1 = "'"+criteria_1+"'"
  

    sql = ("SELECT max(created_at) FROM %s WHERE account_name = %s " % (db_name,criteria_1))

    conn.execute(sql)
    latest_created_at = conn.fetchone()
    latest_created_at = latest_created_at[0]
    latest_created_at = latest_created_at.strftime("%Y-%m-%d")
    latest_created_at = "'"+latest_created_at+"'"

    sql =  ("SELECT the_date, cash_flow_name, cash_flow_amount, created_at FROM %s WHERE (account_name = %s) and created_at = %s " % (db_name,criteria_1,latest_created_at))
 
    conn.execute(sql)
    
    data_tuple =  conn.fetchall()

    date_index = [i[0] for i in data_tuple]

    columns= ['the_date','cash_flow_name','cash_flow_amount', 'db_created_at']

    temp_df = df.from_records(data_tuple, columns=columns)

    temp_df = temp_df.sort_values('the_date')

    temp_df['cash_flow_amount'] = temp_df['cash_flow_amount'].apply(divide_1000)

    current_date = datetime.today()
    
    current_date = current_date.now().date()
  
    end_date = current_date + timedelta(days=60)

    temp_df = temp_df[(temp_df.the_date >= current_date) & (temp_df.the_date <= end_date)]

    temp_df = temp_df[(temp_df['cash_flow_name'] == 'Starting STI Balance') | (temp_df['cash_flow_name'] == 'Ending STI Balance') | (temp_df['cash_flow_name'] == 'Total CAD Inflow') | (temp_df['cash_flow_name'] == 'Total CAD Outflow')]

    sql = ('TRUNCATE the_zoo.sti_cf_view')

    db_name = 'the_zoo.sti_cf_view'

    conn.execute(sql)
    conn.connection.commit()

    temp_df['number_of_days'] = temp_df['the_date'] - current_date
    temp_df['number_of_days'] = temp_df['number_of_days'].astype('timedelta64[D]').astype(int)

    temp_df = temp_df.replace(to_replace='Total CAD Outflow',value='Outflow')
    temp_df = temp_df.replace(to_replace='Total CAD Inflow',value='Inflow')
    temp_df = temp_df.replace(to_replace='Starting STI Balance',value='Starting Balance')
    temp_df = temp_df.replace(to_replace='Ending STI Balance',value='Ending Balance')

    columns= ['the_date','cash_flow_name','cash_flow_amount','db_created_at','number_of_days']

    upload_to_db(db_name,columns,temp_df, conn)
  
def cad_operations_cf_view(conn,db_name):

    criteria_1 = 'Operating CAD'
    criteria_1 = "'"+criteria_1+"'"
  

    sql = ("SELECT max(created_at) FROM %s WHERE account_name = %s " % (db_name,criteria_1))

    conn.execute(sql)
    latest_created_at = conn.fetchone()
    latest_created_at = latest_created_at[0]
    latest_created_at = latest_created_at.strftime("%Y-%m-%d")
    latest_created_at = "'"+latest_created_at+"'"

    sql =  ("SELECT the_date, cash_flow_name, cash_flow_amount, created_at FROM %s WHERE (account_name = %s) and created_at = %s " % (db_name,criteria_1,latest_created_at))

    conn.execute(sql)
    
    data_tuple =  conn.fetchall()

    date_index = [i[0] for i in data_tuple]

    columns= ['the_date','cash_flow_name','cash_flow_amount', 'db_created_at']

    temp_df = df.from_records(data_tuple, columns=columns)

    temp_df = temp_df.sort_values('the_date')

    temp_df['cash_flow_amount'] = temp_df['cash_flow_amount'].apply(divide_1000)

    current_date = datetime.today()
    
    current_date = current_date.now().date()
  
    end_date = current_date + timedelta(days=60)

    temp_df = temp_df[(temp_df.the_date >= current_date) & (temp_df.the_date <= end_date)]

    temp_df = temp_df[(temp_df['cash_flow_name'] == 'Starting CAD Cash') | (temp_df['cash_flow_name'] == 'Ending CAD Cash') | (temp_df['cash_flow_name'] == 'Total CAD Inflow') | (temp_df['cash_flow_name'] == 'Total CAD Outflow')]

    sql = ('TRUNCATE the_zoo.operating_cad_cf')

    db_name = 'the_zoo.operating_cad_cf'

    conn.execute(sql)
    conn.connection.commit()

    temp_df['number_of_days'] = temp_df['the_date'] - current_date
    temp_df['number_of_days'] = temp_df['number_of_days'].astype('timedelta64[D]').astype(int)

    temp_df = temp_df.replace(to_replace='Total CAD Outflow',value='Outflow')
    temp_df = temp_df.replace(to_replace='Total CAD Inflow',value='Inflow')
    temp_df = temp_df.replace(to_replace='Starting CAD Balance',value='Starting Balance')
    temp_df = temp_df.replace(to_replace='Ending CAD Balance',value='Ending Balance')

    columns= ['the_date','cash_flow_name','cash_flow_amount','db_created_at','number_of_days']

    upload_to_db(db_name,columns,temp_df, conn)

def usd_operations_cf_view(conn,db_name):

    criteria_1 = 'Operating USD'
    criteria_1 = "'"+criteria_1+"'"
  

    sql = ("SELECT max(created_at) FROM %s WHERE account_name = %s " % (db_name,criteria_1))

    conn.execute(sql)
    latest_created_at = conn.fetchone()
    latest_created_at = latest_created_at[0]
    latest_created_at = latest_created_at.strftime("%Y-%m-%d")
    latest_created_at = "'"+latest_created_at+"'"

    sql =  ("SELECT the_date, cash_flow_name, cash_flow_amount, created_at FROM %s WHERE (account_name = %s) and created_at = %s " % (db_name,criteria_1,latest_created_at))

    conn.execute(sql)
    
    data_tuple =  conn.fetchall()

    date_index = [i[0] for i in data_tuple]

    columns= ['the_date','cash_flow_name','cash_flow_amount', 'db_created_at']

    temp_df = df.from_records(data_tuple, columns=columns)

    temp_df = temp_df.sort_values('the_date')

    temp_df['cash_flow_amount'] = temp_df['cash_flow_amount'].apply(divide_1000)

    current_date = datetime.today()
    
    current_date = current_date.now().date()
  
    end_date = current_date + timedelta(days=60)

    temp_df = temp_df[(temp_df.the_date >= current_date) & (temp_df.the_date <= end_date)]

    temp_df = temp_df[(temp_df['cash_flow_name'] == 'Starting USD Cash') | (temp_df['cash_flow_name'] == 'Ending USD Cash') | (temp_df['cash_flow_name'] == 'Total USD Inflow') | (temp_df['cash_flow_name'] == 'Total USD Outflow')]

    sql = ('TRUNCATE the_zoo.operating_usd_cf')

    db_name = 'the_zoo.operating_usd_cf'

    conn.execute(sql)
    conn.connection.commit()

    temp_df['number_of_days'] = temp_df['the_date'] - current_date
    temp_df['number_of_days'] = temp_df['number_of_days'].astype('timedelta64[D]').astype(int)

    temp_df = temp_df.replace(to_replace='Total USD Outflow',value='Outflow')
    temp_df = temp_df.replace(to_replace='Total USD Inflow',value='Inflow')
    temp_df = temp_df.replace(to_replace='Starting USD Balance',value='Starting Balance')
    temp_df = temp_df.replace(to_replace='Ending USD Balance',value='Ending Balance')

    columns= ['the_date','cash_flow_name','cash_flow_amount','db_created_at','number_of_days']

    upload_to_db(db_name,columns,temp_df, conn)

def main():

    start_time = time.time()

    # from user_credentials file
    host_name = database_credentials.host_name 
    port = database_credentials.port
    username = database_credentials.username
    password = database_credentials.password
    database = database_credentials.database


    conn = connect_to_database(host_name,port,username,password,database)

    sti_cf_view(conn,db_name='the_zoo.sti_cash_flows')
    cad_operations_cf_view(conn,db_name='the_zoo.sti_cash_flows')
    usd_operations_cf_view(conn,db_name='the_zoo.sti_cash_flows')


    end_time = time.time()
    time_elapsed = int(end_time - start_time)
    minutes, seconds = time_elapsed // 60, time_elapsed % 60

    print("Processing time is " + str(minutes) + ":" + str(seconds).zfill(2))
