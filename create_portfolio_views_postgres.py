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
    
    for idx in temp_df.index:

        data = temp_df.loc[idx]

        data_list = data.tolist()
 
        #function call
        data_list_2 = tuple(data_list)

        conn.execute('INSERT INTO '+table_name+ ' ('+ column_string +') VALUES (' + input_string + ');', (data_list_2))

    conn.connection.commit()

def upload_to_db_maturity_bucket(conn,temp_df,table_name):
    

    sql = ('TRUNCATE ' + table_name)

    db_name = table_name

    conn.execute(sql)
    conn.connection.commit()

    columns = get_column_names(conn,table_name)

    #function call
    column_string = column_str(columns)
    #function call
    input_string = input_str(len(columns))

    for idx in temp_df.index:

        data = temp_df.loc[idx]
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
    ticker_temp_df.loc[:,'security_bloomberg_ticker_dup'] = ticker_temp_df.duplicated()
    duplicate_number = max(ticker_temp_df[ticker_temp_df.security_bloomberg_ticker_dup == True].count())
    if duplicate_number != 0:
        print ticker_temp_df[ticker_temp_df.security_bloomberg_ticker_dup == True]
        sys.exit("ERROR");

def join_type_view_df(daily_mv_df,ticker_information_df,issuer_df):

    ticker_temp_df = ticker_information_df[['security_bloomberg_ticker','issuer_name','annualized_yield','maturity_date','category']]
    check_security_duplicates(ticker_temp_df)

    ticker_temp_df = ticker_temp_df.drop('security_bloomberg_ticker_dup', axis=1)
 
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

    if row.days_to_maturity == 0:
        return 'Cash'
    if row.days_to_maturity == 1:
        return 'Overnight'
    elif row.days_to_maturity > 1 and row.days_to_maturity <= 5:
        return '2-5'
    elif row.days_to_maturity > 5 and row.days_to_maturity <= 10:
        return '6-10'   
    elif row.days_to_maturity > 10 and row.days_to_maturity <= 20:
        return '11-20'  
    elif row.days_to_maturity > 20 and row.days_to_maturity <= 30:
        return '21-30'  
    elif row.days_to_maturity > 30 and row.days_to_maturity <= 45:
        return '31-45'  
    elif row.days_to_maturity > 45 and row.days_to_maturity <= 60:
        return '46-60'  
    elif row.days_to_maturity > 60 and row.days_to_maturity <= 90:
        return '61-90'  
    elif row.days_to_maturity > 90:
        return '>91'  


def maturity_bucket_view(conn,daily_mv_df,ticker_information_df,table_name):

    for idx in daily_mv_df.index:
        maturity_date = daily_mv_df.loc[idx, 'maturity_date'] 
        the_date = daily_mv_df.loc[idx,'the_date']
        the_date = the_date.date()
        days_to_maturity = maturity_date - the_date

        daily_mv_df.loc[idx,'days_to_maturity'] = days_to_maturity

    daily_mv_df.days_to_maturity[daily_mv_df['security_bloomberg_ticker']== 'Cash'] = 1

    daily_mv_df['days_to_maturity'] = daily_mv_df['days_to_maturity'].astype('timedelta64[D]').astype(int)

    daily_mv_df['maturity_bucket'] = None

    daily_mv_df['maturity_bucket'] = daily_mv_df.apply(maturity_bucket, axis=1)

    grouped = daily_mv_df.groupby(by=['the_date','maturity_bucket']).sum()
  
    temp_df = df(grouped)


    temp_df.reset_index(inplace=True)

    temp_df['market_value'] = temp_df['market_value'] / 1000000

    temp_df['cash_flow_name'] = 'STI Maturity'


    rolling_sti_market = daily_mv_df[['the_date','market_value','days_to_maturity','maturity_bucket']]

    rolling_sti_market = rolling_sti_market[rolling_sti_market['the_date'] == max(rolling_sti_market['the_date'])]

    grouped = rolling_sti_market.groupby(by=['days_to_maturity']).sum()

    rolling_sti_market = df(grouped)

    days_index = range(0,61)

     
    rolling_sti_market.sort_index(inplace=True)

    rolling_index = rolling_sti_market.index.values

    rolling_index = [x for x in rolling_index]


    unique_days = [x for x in days_index if x not in rolling_index]

    days_index_df = df(index=unique_days)
    
    rolling_sti_market = rolling_sti_market.append(days_index_df,ignore_index=False)
    
    rolling_sti_market.sort_index(axis=0,inplace=True)

    rolling_sti_market['STI Maturity'] = rolling_sti_market['market_value'].cumsum()


    rolling_sti_market.fillna(value=None,method='ffill', inplace=True)

    rolling_sti_market['cash_flow_name'] = 'STI Maturity'

    rolling_sti_market.drop(['market_value'], axis=1, inplace=True)

    rolling_sti_market.rename(columns={'STI Maturity': 'cumulative_cash'}, inplace=True)

    rolling_sti_market.reset_index(inplace=True)

    rolling_sti_market['cumulative_cash'] = rolling_sti_market['cumulative_cash'] / 1000000


    temp_df = temp_df[['the_date','cash_flow_name','maturity_bucket','market_value','percentage']]

    bucket_array = ['Overnight', '2-5', '6-10','11-20','21-30','31-45','46-60','61-90','>91']

    latest_date = max(temp_df['the_date'])

    latest_date_df = temp_df[temp_df['the_date'] == latest_date]

    latest_maturity_bucket_array = latest_date_df['maturity_bucket'].values
    
    added_maturity_bucket_array = [x for x in bucket_array if x not in latest_maturity_bucket_array]


    rows_added = len(added_maturity_bucket_array)
    
    #build data for df
    input_array = []
    for count in range(0,rows_added):
        loop_array = [latest_date,added_maturity_bucket_array[count],None,None]
        input_array.append(loop_array)
    
    append_df = df(input_array, columns=['the_date','maturity_bucket','market_value','percentage'])

    append_df['cash_flow_name'] = 'STI Maturity'
    temp_df = temp_df.append(append_df, ignore_index='True')

    
    temp_df['market_value'][temp_df['market_value'] == 'holder'] = None
    temp_df.loc[:,'percentage'][temp_df.loc[:,'percentage'] == 'holder'] = None

    
    sti_cf = sti_cf_view(conn,'the_zoo.sti_cash_flows')

    sti_cf.sort(columns='the_date', inplace=True)

    sti_cf['days_to_maturity'] = sti_cf['the_date'] - date.today()

    sti_cf['days_to_maturity'] = sti_cf['days_to_maturity'].astype('timedelta64[D]').astype(int)

    sti_cf = sti_cf[sti_cf['days_to_maturity'] < 61]

    sti_cf['maturity_bucket'] = None

    sti_cf['maturity_bucket'] = sti_cf.apply(maturity_bucket, axis=1)
    grouped = sti_cf.groupby(by=['the_date','cash_flow_name','maturity_bucket']).sum()

    sti_cf = df(grouped)

    sti_cf.reset_index(inplace=True)

    sti_cf = sti_cf[(sti_cf['cash_flow_name'] == 'Transfer From STI') | (sti_cf['cash_flow_name'] == 'Transfer To STI')]

    rolling_sti_transfer_from_sti = sti_cf[sti_cf['cash_flow_name'] == 'Transfer From STI']


    rolling_sti_transfer_from_sti.set_index(['days_to_maturity'], inplace=True)


    rolling_index = rolling_sti_transfer_from_sti.index.values

    rolling_index = [x for x in rolling_index]


    unique_days = [x for x in days_index if x not in rolling_index]

    days_index_df = df(index=unique_days)
    
    rolling_sti_transfer_from_sti = rolling_sti_transfer_from_sti.append(days_index_df,ignore_index=False)

    
    rolling_sti_transfer_from_sti.sort_index(axis=0,inplace=True)

    rolling_sti_transfer_from_sti.drop(['maturity_bucket','the_date'], axis=1, inplace=True)


    rolling_sti_transfer_from_sti['cumulative_cash'] = rolling_sti_transfer_from_sti['cash_flow_amount'].cumsum()

    rolling_sti_transfer_from_sti.drop(['cash_flow_amount'], axis=1, inplace=True)

    rolling_sti_transfer_from_sti['cumulative_cash'] = rolling_sti_transfer_from_sti['cumulative_cash'] / 1000

    rolling_sti_transfer_from_sti.reset_index(inplace=True)

    rolling_sti_transfer_from_sti.fillna(0, inplace=True)


    rolling_sti_transfer_to_sti = sti_cf[sti_cf['cash_flow_name'] == 'Transfer To STI']

    rolling_sti_transfer_to_sti.set_index(['days_to_maturity'], inplace=True)

    rolling_index = rolling_sti_transfer_to_sti.index.values

    rolling_index = [x for x in rolling_index]


    unique_days = [x for x in days_index if x not in rolling_index]

    days_index_df = df(index=unique_days)
    
    rolling_sti_transfer_to_sti = rolling_sti_transfer_to_sti.append(days_index_df,ignore_index=False)

    
    rolling_sti_transfer_to_sti.sort_index(axis=0,inplace=True)

    rolling_sti_transfer_to_sti.drop(['maturity_bucket','the_date'], axis=1, inplace=True)


    rolling_sti_transfer_to_sti['cumulative_cash'] = rolling_sti_transfer_to_sti['cash_flow_amount'].cumsum()

    rolling_sti_transfer_to_sti.drop(['cash_flow_amount'], axis=1, inplace=True)

    rolling_sti_transfer_to_sti['cumulative_cash'] = rolling_sti_transfer_to_sti['cumulative_cash'] / 1000

    rolling_sti_transfer_to_sti.reset_index(inplace=True)

    rolling_sti_transfer_to_sti.fillna(0, inplace=True)
    
    rolling_cumulative_cash = pd.concat([rolling_sti_market,rolling_sti_transfer_from_sti,rolling_sti_transfer_to_sti], axis=0)

    temp_df = temp_df[['the_date','cash_flow_name','maturity_bucket','market_value','percentage']]


    sti_cf['maturity_bucket'] = sti_cf.apply(maturity_bucket, axis=1)

    grouped = sti_cf.groupby(by=['cash_flow_name','maturity_bucket']).sum()

    sti_cf = df(grouped)

    temp_df.reset_index(inplace=True)

    sti_cf.reset_index(inplace=True)
      
   
    sti_cf.rename(columns={'cash_flow_amount': 'market_value'}, inplace=True)

    temp_df = temp_df[['the_date','cash_flow_name','maturity_bucket','market_value','percentage']]

    upload_to_db(conn,temp_df,table_name)

   
    temp_df = temp_df[temp_df['the_date'] == max(temp_df['the_date'])]

    sti_cf.drop(['days_to_maturity'], axis=1, inplace=True)
    sti_cf['market_value'] = sti_cf['market_value'] /1000
    temp_df.drop(['the_date','percentage'], axis=1, inplace=True)

    temp_df = temp_df.append(sti_cf)

    temp_df['market_value'][temp_df['cash_flow_name'] == 'Transfer To STI'] = temp_df['market_value'][temp_df['cash_flow_name'] == 'Transfer To STI'] * -1

    temp_df['market_value'] = np.where(temp_df['market_value'] == 0, None, temp_df['market_value'])

    upload_to_db_maturity_bucket(conn,temp_df,'the_zoo.sti_current_maturity_bucket')

    rolling_cumulative_cash.reset_index(inplace=True)

    rolling_cumulative_cash['days_to_maturity'] = rolling_cumulative_cash[['days_to_maturity']].astype(int)

    rolling_cumulative_cash.drop('index',axis=1,inplace=True)

    rolling_cumulative_cash.reset_index()


    rolling_cumulative_cash['cumulative_cash'][rolling_cumulative_cash['cash_flow_name'] == 'Transfer To STI'] = rolling_cumulative_cash['cumulative_cash'][rolling_cumulative_cash['cash_flow_name'] == 'Transfer To STI'] * -1

    #upload_to_db_maturity_bucket(conn,rolling_cumulative_cash,'the_zoo.sti_rolling_cumulative_cf')
    diff_df = rolling_cumulative_cash

    diff_df['cumulative_cash'][diff_df['cash_flow_name'] == 'Transfer From STI'] = diff_df['cumulative_cash'][diff_df['cash_flow_name'] == 'Transfer From STI'] * -1
    diff_df = rolling_cumulative_cash.pivot(index='days_to_maturity', columns='cash_flow_name').swaplevel(1,0,axis=1)

    temp_series = diff_df.sum(axis=1)

    

    surplus_def = df(temp_series)

    surplus_def['cash_flow_name'] = 'Surplus/Deficit'

    surplus_def.columns = ['cumulative_cash','cash_flow_name']

    surplus_def.reset_index(inplace=True)

   
    rolling_cumulative_cash = rolling_cumulative_cash.append(surplus_def,ignore_index=True)


    rolling_cumulative_cash['days_to_maturity'] = rolling_cumulative_cash[['days_to_maturity']].astype(int)

    rolling_cumulative_cash['cumulative_cash'][rolling_cumulative_cash['cash_flow_name'] == 'Transfer From STI'] = rolling_cumulative_cash['cumulative_cash'][rolling_cumulative_cash['cash_flow_name'] == 'Transfer From STI'] * -1
    rolling_cumulative_cash['cumulative_cash'] = np.where(rolling_cumulative_cash['cumulative_cash'] == 0, None, rolling_cumulative_cash['cumulative_cash'])

    
    upload_to_db_maturity_bucket(conn,rolling_cumulative_cash,'the_zoo.sti_rolling_cumulative_cf')
def top_five_holdings(conn,daily_mv_df,ticker_information_df,table_name) :

    current_df = daily_mv_df[daily_mv_df['the_date'] == max(daily_mv_df['the_date'])]

    current_df = current_df.sort('market_value', ascending=False)

    current_df = current_df[:5]

    current_df['days_to_maturity'] = current_df['maturity_date'] - date.today()

    current_df['days_to_maturity'] = current_df['days_to_maturity'].astype('timedelta64[D]').astype(int)

    current_df['market_value'] = current_df['market_value']  / 1000000

    current_df = current_df[['issuer_name','category','market_value','percentage','maturity_date','days_to_maturity']]

    upload_to_db_maturity_bucket(conn,current_df,table_name)


def sti_cf_view(conn,db_name):

    criteria_1 = 'Starting STI Balance'
    criteria_1 = "'"+criteria_1+"'"
    criteria_2 = 'Ending STI Balance'
    criteria_2 = "'"+criteria_2+"'"
    criteria_3 = 'Transfer From STI'
    criteria_3 = "'"+criteria_3+"'"
    criteria_4 = 'Transfer To STI'
    criteria_4 = "'"+criteria_4+"'"
    criteria_5 = 'Transfer FROM STI'
    criteria_5 = "'"+criteria_5+"'"

    sql = ("SELECT max(created_at) FROM %s WHERE cash_flow_name = %s OR cash_flow_name = %s OR cash_flow_name = %s OR cash_flow_name = %s OR cash_flow_name = %s" % 
        (db_name,criteria_1,criteria_2,criteria_3, criteria_4,criteria_5))

    conn.execute(sql)
    latest_created_at = conn.fetchone()
    latest_created_at = latest_created_at[0]
    latest_created_at = latest_created_at.strftime("%Y-%m-%d")
    latest_created_at = "'"+latest_created_at+"'"

    sql =  ("SELECT the_date, cash_flow_name, cash_flow_amount, created_at FROM %s WHERE (cash_flow_name = %s OR cash_flow_name = %s OR cash_flow_name =  %s OR cash_flow_name =  %s OR cash_flow_name =  %s) and created_at = %s " % 
                (db_name,criteria_1, criteria_2, criteria_3, criteria_4,criteria_5, latest_created_at))
 
    conn.execute(sql)
    
    data_tuple =  conn.fetchall()

    date_index = [i[0] for i in data_tuple]

    columns= ['the_date','cash_flow_name','cash_flow_amount','db_created_at']

    temp_df = df.from_records(data_tuple, columns=columns)

    temp_df = temp_df.sort('the_date')

    return temp_df

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
    #top_five_holdings(conn,daily_mv_df,ticker_information_df, table_name = 'the_zoo.sti_top_five_holdings_current')
    #portfolio_charateristics(conn, daily_mv_df,ticker_information_df, table_name = )


    end_time = time.time()
    time_elapsed = int(end_time - start_time)
    minutes, seconds = time_elapsed // 60, time_elapsed % 60

    print("Processing time is " + str(minutes) + ":" + str(seconds).zfill(2))

main()
