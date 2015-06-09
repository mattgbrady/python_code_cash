import pandas as pd
from pandas import DataFrame as df
import psycopg2
import numpy as np
import time
from datetime import date
from datetime import datetime
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
      
        #function call
        data_list_2 = tuple(data_list)

        conn.execute('INSERT INTO '+table_name+ ' ('+ column_string +') VALUES (' + input_string + ');', (data_list_2))

    conn.connection.commit()

def sti_cf_view(conn,db_name):

    criteria_1 = 'STI Balance'
    criteria_1 = "'"+criteria_1+"'"
    criteria_2 = 'Transfer From STI'
    criteria_2 = "'"+criteria_2+"'"
    criteria_3 = 'Transfer To STI'
    criteria_3 = "'"+criteria_3+"'"
    criteria_4 = 'Transfer FROM STI'
    criteria_4 = "'"+criteria_4+"'"

    sql = ("SELECT max(created_at) FROM %s WHERE cash_flow_name = %s OR cash_flow_name = %s OR cash_flow_name = %s OR cash_flow_name = %s" % 
        (db_name,criteria_1,criteria_2,criteria_3, criteria_4))

    conn.execute(sql)
    latest_created_at = conn.fetchone()
    latest_created_at = latest_created_at[0]
    latest_created_at = latest_created_at.strftime("%Y-%m-%d")
    latest_created_at = "'"+latest_created_at+"'"

    sql =  ("SELECT the_date, cash_flow_name, cash_flow_amount, created_at FROM %s WHERE (cash_flow_name = %s OR cash_flow_name = %s OR cash_flow_name =  %s OR cash_flow_name =  %s) and created_at = %s " % 
                (db_name,criteria_1, criteria_2, criteria_3, criteria_4, latest_created_at))
 
    conn.execute(sql)
    
    data_tuple =  conn.fetchall()

    date_index = [i[0] for i in data_tuple]

    columns= ['the_date','cash_flow_name','cash_flow_amount', 'db_created_at']

    temp_df = df.from_records(data_tuple, columns=columns)

    temp_df = temp_df.sort('the_date')

    current_date = datetime.today()
    
    current_date = current_date.now().date()
  
    end_date = current_date + timedelta(days=60)

    temp_df = temp_df[(temp_df.the_date >= current_date) & (temp_df.the_date <= end_date)]

    
    sql = ('TRUNCATE the_zoo.sti_cf_view')

    db_name = 'the_zoo.sti_cf_view'

    conn.execute(sql)
    conn.connection.commit()

    temp_df['number_of_days'] = temp_df['the_date'] - current_date
    temp_df['number_of_days'] = temp_df['number_of_days'].astype('timedelta64[D]').astype(int)

    temp_df = temp_df.replace(to_replace='Transfer From STI',value='Outflow')
    temp_df = temp_df.replace(to_replace='Transfer To STI',value='Inflow')

    temp_df['cash_flow_amount'][temp_df['cash_flow_name'] != 'STI Balance'] = temp_df['cash_flow_amount'][temp_df['cash_flow_name'] != 'STI Balance'] * -1


    columns= ['the_date','cash_flow_name','cash_flow_amount','db_created_at','number_of_days']


    upload_to_db(db_name,columns,temp_df, conn)
  
def operations_cf_view(conn,db_name):

    criteria_1 = 'Ending CAD Cash'
    criteria_1 = "'"+criteria_1+"'"
    criteria_2 = 'Ending CAD Cash'
    criteria_2 = "'"+criteria_2+"'"
    criteria_3 = 'Total CAD Inflow'
    criteria_3 = "'"+criteria_3+"'"
    criteria_4 = 'Total CAD Outflow'
    criteria_4 = "'"+criteria_4+"'"

    sql = ("SELECT max(created_at) FROM %s WHERE cash_flow_name = %s OR cash_flow_name = %s OR cash_flow_name = %s OR cash_flow_name = %s" % 
        (db_name,criteria_1,criteria_2,criteria_3, criteria_4))

    conn.execute(sql)
    latest_created_at = conn.fetchone()
    latest_created_at = latest_created_at[0]
    latest_created_at = latest_created_at.strftime("%Y-%m-%d")
    latest_created_at = "'"+latest_created_at+"'"

    sql =  ("SELECT the_date, cash_flow_name, cash_flow_amount, created_at FROM %s WHERE (cash_flow_name = %s OR cash_flow_name = %s OR cash_flow_name =  %s OR cash_flow_name =  %s) and created_at = %s " % 
                (db_name,criteria_1, criteria_2, criteria_3, criteria_4, latest_created_at))
 
    conn.execute(sql)
    
    data_tuple =  conn.fetchall()

    date_index = [i[0] for i in data_tuple]

    columns= ['the_date','cash_flow_name','cash_flow_amount', 'db_created_at']

    temp_df = df.from_records(data_tuple, columns=columns)

    temp_df = temp_df.sort('the_date')

    current_date = datetime.today()
    
    current_date = current_date.now().date()
  
    end_date = current_date + timedelta(days=60)

    temp_df = temp_df[(temp_df.the_date >= current_date) & (temp_df.the_date <= end_date)]

    
    sql = ('TRUNCATE the_zoo.operating_cad_cf')

    db_name = 'the_zoo.operating_cad_cf'

    conn.execute(sql)
    conn.connection.commit()

    temp_df['number_of_days'] = temp_df['the_date'] - current_date
    temp_df['number_of_days'] = temp_df['number_of_days'].astype('timedelta64[D]').astype(int)

    temp_df = temp_df.replace(to_replace='Total CAD Outflow',value='Outflow')
    temp_df = temp_df.replace(to_replace='Total CAD Inflow',value='Inflow')
    temp_df = temp_df.replace(to_replace='Ending CAD Cash',value='CAD Operating Balance')

    columns= ['the_date','cash_flow_name','cash_flow_amount','db_created_at','number_of_days','flag_below_zero']
    


    temp_df['flag_below_zero'] = np.nan

    temp_df['flag_below_zero'][(temp_df['cash_flow_name'] == 'CAD Operating Balance') & (temp_df['cash_flow_amount'] < 0)] = temp_df['cash_flow_amount'][(temp_df['cash_flow_name'] == 'CAD Operating Balance') & (temp_df['cash_flow_amount'] < 0)].values


    
    for index in temp_df.index:
        print temp_df.loc[index]
    #temp_df['flag_below_zero'].replace(str('nan'),None, inplace=True)
  
    #print temp_df
    #upload_to_db(db_name,columns,temp_df, conn)



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

    #sti_cf_view(conn,db_name='the_zoo.sti_cash_flows')
    operations_cf_view(conn,db_name='the_zoo.sti_cash_flows')


    end_time = time.time()
    time_elapsed = int(end_time - start_time)
    minutes, seconds = time_elapsed // 60, time_elapsed % 60

    print("Processing time is " + str(minutes) + ":" + str(seconds).zfill(2))



main()
