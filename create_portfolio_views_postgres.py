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

    date_look_back =max(temp_df.loc[:,'the_date'])-timedelta(days=days_look_back)

    if min_date <= date_look_back:
        temp_df = temp_df.loc[temp_df.loc[:,'the_date'] >=  date_look_back]

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

    ticker_temp_df.loc[:,'ticker_dup'] = ticker_temp_df.duplicated()

    duplicate_number = max(ticker_temp_df.loc[ticker_temp_df.loc[:,'ticker_dup'] == True].count())

    if duplicate_number != 0:
        print ticker_temp_df.loc[ticker_temp_df.loc[:,'ticker_dup' == True]]
        sys.exit("ERROR");

def join_type_view_df(daily_mv_df,ticker_information_df,issuer_df):

    ticker_temp_df = ticker_information_df[['ticker','issuer_name','annualized_yield','maturity_date','category']].copy()

    check_security_duplicates(ticker_temp_df)

    ticker_temp_df = ticker_temp_df.drop('ticker_dup', axis=1)
 
    daily_mv_df.reset_index(inplace=True)

    ticker_temp_df.set_index('ticker', inplace=True)

    daily_mv_df.join(ticker_temp_df, on ='ticker')

    daily_mv_df = daily_mv_df.join(ticker_temp_df, on ='ticker')

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
        return 'Today'
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
    #elif row.days_to_maturity > 60 and row.days_to_maturity <= 90:
    #    return '61-90'  
    elif row.days_to_maturity > 60:
        return '>60'  


def maturity_bucket_view(conn,daily_mv_df,ticker_information_df,table_name):


    
    cash_available = created_df_from_postgres(conn,'the_zoo.sti_cash_available')

    cash_available.drop(['created_at','id'], axis=1, inplace=True)

    cash_available.rename(columns={'cash_available': 'market_value'}, inplace=True)

    cash_available['ticker'] = 'Cash Available'

    cash_available['the_date'] = pd.to_datetime(cash_available['the_date'])

    daily_mv_df = daily_mv_df.append(cash_available)

    daily_mv_df.reset_index(inplace=True)

    daily_mv_df.drop(['index'], axis=1, inplace=True)

    for idx in daily_mv_df.index:
        ticker_flag = daily_mv_df.loc[idx, 'ticker'] 
        if ticker_flag == 'Cash Available':
            daily_mv_df.loc[idx,'days_to_maturity'] = 0
        else:
            maturity_date = daily_mv_df.loc[idx, 'maturity_date'] 
            the_date = daily_mv_df.loc[idx,'the_date']
            the_date = the_date.date()
            days_to_maturity = maturity_date - the_date

            daily_mv_df.loc[idx,'days_to_maturity'] = days_to_maturity

    data_copy = daily_mv_df.copy()  


    data_copy.loc[:,'days_to_maturity'] = data_copy.loc[:,'days_to_maturity'].astype('timedelta64[D]').astype(int)

    data_copy.loc[data_copy.loc[:,'ticker'] == 'Cash', 'days_to_maturity'] = 0


    data_copy['maturity_bucket'] = None
 
    data_copy.loc[:,'maturity_bucket'] = data_copy.apply(maturity_bucket, axis=1)

    grouped = data_copy.groupby(by=['the_date','maturity_bucket']).sum()
  
    temp_df = df(grouped)


    temp_df.reset_index(inplace=True)

    temp_df.loc[:,'market_value'] = temp_df.loc[:,'market_value'] / 1000000



    temp_df.loc[(temp_df.loc[:,'maturity_bucket'] !=  'Today', 'cash_flow_name')] = 'STI Maturity'
    temp_df.loc[(temp_df.loc[:,'maturity_bucket'] ==  'Today', 'cash_flow_name')] = 'Available Cash'

    rolling_sti_market = data_copy[['the_date','market_value','days_to_maturity','maturity_bucket']].copy()

    rolling_sti_market = rolling_sti_market.loc[rolling_sti_market.loc[:,'the_date'] == max(rolling_sti_market.loc[:,'the_date'])]

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

    rolling_sti_market.loc[:,'STI Maturity'] = rolling_sti_market.loc[:,'market_value'].cumsum()

    rolling_sti_market_fill_zero = rolling_sti_market.copy()


    rolling_sti_market.fillna(value=None,method='ffill', inplace=True)

    rolling_sti_market.loc[:,'cash_flow_name'] = 'STI Maturity'

    rolling_sti_market.drop(['market_value'], axis=1, inplace=True)

    rolling_sti_market.rename(columns={'STI Maturity': 'cumulative_cash'}, inplace=True)

    rolling_sti_market.reset_index(inplace=True)

    rolling_sti_market.loc[:,'cumulative_cash'] = rolling_sti_market.loc[:,'cumulative_cash'] / 1000000


    temp_df = temp_df[['the_date','cash_flow_name','maturity_bucket','market_value','percentage']].copy()

    bucket_array = ['Overnight', '2-5', '6-10','11-20','21-30','31-45','46-60','>60']

    latest_date = max(temp_df['the_date'])

    latest_date_df = temp_df.loc[temp_df.loc[:,'the_date'] == latest_date]

    latest_maturity_bucket_array = latest_date_df['maturity_bucket'].values
    
    added_maturity_bucket_array = [x for x in bucket_array if x not in latest_maturity_bucket_array]


    rows_added = len(added_maturity_bucket_array)
    
    #build data for df
    input_array = []
    for count in range(0,rows_added):
        loop_array = [latest_date,added_maturity_bucket_array[count],None,None]
        input_array.append(loop_array)
    
    append_df = df(input_array, columns=['the_date','maturity_bucket','market_value','percentage'])

    append_df.loc[:,'cash_flow_name'] = 'STI Maturity'
    temp_df = temp_df.append(append_df, ignore_index='True')

    
    temp_df.loc[temp_df.loc[:,'market_value'] == 'holder','market_value'] = None
    temp_df.loc[temp_df.loc[:,'percentage'] == 'holder','percentage'] = None

    
    sti_cf = sti_cf_view(conn,'the_zoo.sti_cash_flows')

    sti_cf.sort(columns='the_date', inplace=True)

    sti_cf.loc[:,'days_to_maturity'] = sti_cf.loc[:,'the_date'] - date.today()

    sti_cf.loc[:,'days_to_maturity'] = sti_cf.loc[:,'days_to_maturity'].astype('timedelta64[D]').astype(int)

    sti_cf = sti_cf.loc[sti_cf.loc[:,'days_to_maturity'] < 61]

    sti_cf.loc[:,'maturity_bucket'] = None

    sti_cf.loc[:,'maturity_bucket'] = sti_cf.apply(maturity_bucket, axis=1)
    grouped = sti_cf.groupby(by=['the_date','cash_flow_name','maturity_bucket']).sum()

    sti_cf = df(grouped)

    sti_cf.reset_index(inplace=True)

    sti_cf = sti_cf.loc[(sti_cf.loc[:,'cash_flow_name'] == 'Transfer From STI') | (sti_cf.loc[:,'cash_flow_name'] == 'Transfer To STI')]

    rolling_sti_transfer_from_sti = sti_cf.loc[sti_cf.loc[:,'cash_flow_name'] == 'Transfer From STI']


    rolling_sti_transfer_from_sti.set_index(['days_to_maturity'], inplace=True)


    rolling_index = rolling_sti_transfer_from_sti.index.values

    rolling_index = [x for x in rolling_index]


    unique_days = [x for x in days_index if x not in rolling_index]

    days_index_df = df(index=unique_days)
    
    rolling_sti_transfer_from_sti = rolling_sti_transfer_from_sti.append(days_index_df,ignore_index=False)

    
    rolling_sti_transfer_from_sti.sort_index(axis=0,inplace=True)

    rolling_sti_transfer_from_sti.drop(['maturity_bucket','the_date'], axis=1, inplace=True)


    rolling_sti_transfer_from_sti.loc[:,'cumulative_cash'] = rolling_sti_transfer_from_sti.loc[:,'cash_flow_amount'].cumsum()

    rolling_sti_transfer_from_sti.drop(['cash_flow_amount'], axis=1, inplace=True)

    rolling_sti_transfer_from_sti.loc[:,'cumulative_cash'] = rolling_sti_transfer_from_sti['cumulative_cash'] / 1000

    rolling_sti_transfer_from_sti.reset_index(inplace=True)

    rolling_sti_transfer_from_sti.fillna(0, inplace=True)


    rolling_sti_transfer_to_sti = sti_cf.loc[sti_cf.loc[:,'cash_flow_name'] == 'Transfer To STI']

    rolling_sti_transfer_to_sti.set_index(['days_to_maturity'], inplace=True)

    rolling_index = rolling_sti_transfer_to_sti.index.values

    rolling_index = [x for x in rolling_index]


    unique_days = [x for x in days_index if x not in rolling_index]

    days_index_df = df(index=unique_days)
    
    rolling_sti_transfer_to_sti = rolling_sti_transfer_to_sti.append(days_index_df,ignore_index=False)

    
    rolling_sti_transfer_to_sti.sort_index(axis=0,inplace=True)

    rolling_sti_transfer_to_sti.drop(['maturity_bucket','the_date'], axis=1, inplace=True)


    rolling_sti_transfer_to_sti.loc[:,'cumulative_cash'] = rolling_sti_transfer_to_sti.loc[:,'cash_flow_amount'].cumsum()

    rolling_sti_transfer_to_sti.drop(['cash_flow_amount'], axis=1, inplace=True)

    rolling_sti_transfer_to_sti.loc[:,'cumulative_cash'] = rolling_sti_transfer_to_sti.loc[:,'cumulative_cash'] / 1000

    rolling_sti_transfer_to_sti.reset_index(inplace=True)

    rolling_sti_transfer_to_sti.fillna(0, inplace=True)
    
    rolling_cumulative_cash = pd.concat([rolling_sti_market,rolling_sti_transfer_from_sti,rolling_sti_transfer_to_sti], axis=0)

    temp_df = temp_df[['the_date','cash_flow_name','maturity_bucket','market_value','percentage']].copy()



    sti_cf.loc[:,'maturity_bucket'] = sti_cf.apply(maturity_bucket, axis=1)


    grouped = sti_cf.groupby(by=['cash_flow_name','maturity_bucket']).sum()

    sti_cf = df(grouped)

    temp_df.reset_index(inplace=True)

    sti_cf.reset_index(inplace=True)
      
   
    sti_cf.rename(columns={'cash_flow_amount': 'market_value'}, inplace=True)

    temp_df = temp_df[['the_date','cash_flow_name','maturity_bucket','market_value','percentage']].copy()

 
    upload_to_db(conn,temp_df,table_name)

   
    temp_df = temp_df[temp_df['the_date'] == max(temp_df['the_date'])]


    sti_cf.drop(['days_to_maturity'], axis=1, inplace=True)
    sti_cf.loc[:,'market_value'] = sti_cf.loc[:,'market_value'] /1000
    temp_df.drop(['the_date','percentage'], axis=1, inplace=True)

    temp_df = temp_df.append(sti_cf)

    temp_df.loc[temp_df.loc[:,'cash_flow_name'] == 'Transfer To STI','market_value'] = temp_df.loc[temp_df.loc[:,'cash_flow_name'] == 'Transfer To STI','market_value'] * -1

    temp_df.loc[:,'market_value'] = np.where(temp_df.loc[:,'market_value'] == 0, None, temp_df.loc[:,'market_value'])

    temp_df.reset_index(inplace=True)
    temp_df.drop('index',axis=1,inplace=True)


    upload_to_db_maturity_bucket(conn,temp_df,'the_zoo.sti_current_maturity_bucket')

    rolling_cumulative_cash.reset_index(inplace=True)

    rolling_cumulative_cash.loc[:,'days_to_maturity'] = rolling_cumulative_cash[['days_to_maturity']].astype(int)

    rolling_cumulative_cash.drop('index',axis=1,inplace=True)

    rolling_cumulative_cash.reset_index()


    rolling_cumulative_cash.loc[rolling_cumulative_cash.loc[:,'cash_flow_name'] == 'Transfer To STI','cumulative_cash'] = rolling_cumulative_cash.loc[rolling_cumulative_cash.loc[:,'cash_flow_name'] == 'Transfer To STI','cumulative_cash'] * -1

   
    diff_df = rolling_cumulative_cash

    diff_df.loc[diff_df.loc[:,'cash_flow_name'] == 'Transfer From STI','cumulative_cash'] = diff_df.loc[diff_df.loc[:,'cash_flow_name'] == 'Transfer From STI','cumulative_cash'] * -1
    
    diff_df = rolling_cumulative_cash.pivot(index='days_to_maturity', columns='cash_flow_name').swaplevel(1,0,axis=1)

    temp_series = diff_df.sum(axis=1)

    
    surplus_def = df(temp_series)

    surplus_def['cash_flow_name'] = 'Surplus/Deficit'

    surplus_def.columns = ['cumulative_cash','cash_flow_name']

    surplus_def.reset_index(inplace=True)

   
    rolling_cumulative_cash = rolling_cumulative_cash.append(surplus_def,ignore_index=True)


    rolling_cumulative_cash.loc[:,'days_to_maturity'] = rolling_cumulative_cash[['days_to_maturity']].astype(int)

    rolling_cumulative_cash.loc[rolling_cumulative_cash.loc[:,'cash_flow_name'] == 'Transfer From STI','cumulative_cash'] = rolling_cumulative_cash.loc[rolling_cumulative_cash.loc[:,'cash_flow_name'] == 'Transfer From STI','cumulative_cash'] * -1
    rolling_cumulative_cash.loc[:,'cumulative_cash'] = np.where(rolling_cumulative_cash.loc[:,'cumulative_cash'] == 0, None, rolling_cumulative_cash.loc[:,'cumulative_cash'])

    rolling_sti_market_fill_zero = rolling_sti_market_fill_zero['market_value'].copy()
   
    rolling_sti_market_fill_zero.fillna(value=0,inplace=True)

    the_date = np.datetime64(the_date)

    end_date = the_date + np.timedelta64(61,'D')

    date_arr = np.arange(the_date,end_date)

    rolling_sti_market_fill_zero = pd.Series(rolling_sti_market_fill_zero.values, index=date_arr)

    rolling_sti_market_fill_zero = rolling_sti_market_fill_zero.reset_index()

    rolling_sti_market_fill_zero.rename(columns={'index':'the_date',0:'maturity_cash'},inplace=True)

    rolling_sti_market_fill_zero.loc[:,'maturity_cash'] = rolling_sti_market_fill_zero.loc[:,'maturity_cash'] / 1000000

    upload_to_db_maturity_bucket(conn,rolling_cumulative_cash,'the_zoo.sti_rolling_cumulative_cf')

    rolling_cumulative_cash.loc[rolling_cumulative_cash.loc[:,'cash_flow_name'] == 'STI Maturity', 'cash_flow_name'] = 'STI Maturity & Available Cash'

    upload_to_db_maturity_bucket(conn,rolling_cumulative_cash,'the_zoo.sti_rolling_cumulative_cf')
    upload_to_db_maturity_bucket(conn,rolling_sti_market_fill_zero,'the_zoo.sti_maturity_by_date')



def top_five_holdings(conn,daily_mv_df,ticker_information_df,table_name) :

    the_date = max(daily_mv_df['the_date'])

    the_date = the_date.date()

    current_df = daily_mv_df.loc[daily_mv_df.loc[:,'the_date'] == max(daily_mv_df.loc[:,'the_date'])]

    current_df = current_df.sort('market_value', ascending=False)

    current_df.loc[:,'days_to_maturity'] = current_df.loc[:,'maturity_date'] - the_date

    current_df.loc[:,'days_to_maturity'] = current_df.loc[:,'days_to_maturity'].astype('timedelta64[D]').astype(int)

    current_df.loc[current_df.loc[:,'issuer_name'] == 'Cash', 'days_to_maturity'] = 0

    current_df.loc[:,'market_value'] = current_df.loc[:,'market_value']  / 1000000

    current_df = current_df[['issuer_name','category','market_value','percentage','maturity_date','days_to_maturity']]

    top_five_df = current_df[:5].copy()

    upload_to_db_maturity_bucket(conn,top_five_df,table_name)
    upload_to_db_maturity_bucket(conn,current_df,table_name='the_zoo.sti_all_holdings_current')

def portfolio_characteristics(conn,daily_mv_df,ticker_information_df,table_name) :


    for idx in daily_mv_df.index:
        maturity_date = daily_mv_df.loc[idx, 'maturity_date'] 
        the_date = daily_mv_df.loc[idx,'the_date']
        the_date = the_date.date()
        days_to_maturity = maturity_date - the_date
        daily_mv_df.loc[idx,'days_to_maturity'] = days_to_maturity


    daily_mv_df.loc[:,'days_to_maturity'] = daily_mv_df.loc[:,'days_to_maturity'].astype('timedelta64[D]').astype(int)

    daily_mv_df.loc[daily_mv_df.loc[:,'issuer_name'] == 'Cash', 'days_to_maturity'] = 0


    daily_mv_df.loc[:,'weight_yield'] = daily_mv_df.loc[:,'annualized_yield'] * daily_mv_df.loc[:,'percentage']

    weight_yield_group = daily_mv_df.groupby(by=['the_date'])['weight_yield'].sum()
  
    weight_yield_df = df(weight_yield_group)


    holdings = daily_mv_df.groupby('the_date').issuer_name.count()

    holdings = df(holdings)

    holdings.rename(columns={'issuer_name': 'number_of_holdings'}, inplace=True)

    

    final_df = pd.concat([weight_yield_df,holdings], axis=1)

    
    daily_mv_df.loc[:,'weight_maturity'] = daily_mv_df.loc[:,'days_to_maturity'] * daily_mv_df.loc[:,'percentage']

    weighted_mat_group = daily_mv_df.groupby(by=['the_date'])['weight_maturity'].sum()
  
    weight_mat_df = df(weighted_mat_group)


    average_mat_group = daily_mv_df.groupby(by=['the_date'])['days_to_maturity'].mean()
  
    average_mat_group = df(average_mat_group)

    average_mat_group.rename(columns={'days_to_maturity': 'average_maturity'}, inplace=True)

    longest_maturity = daily_mv_df.groupby(by=['the_date'])['days_to_maturity'].max()

    longest_maturity = df(longest_maturity)

    longest_maturity.rename(columns={'days_to_maturity': 'longest_maturity'}, inplace=True)

    final_df = pd.concat([weight_yield_df,weight_mat_df,holdings,average_mat_group,longest_maturity], axis=1)

    percentage_maturing = daily_mv_df.loc[daily_mv_df.loc[:,'days_to_maturity'] <= 5].groupby(by='the_date')['percentage'].sum()

    percentage_maturing = df(percentage_maturing)

    percentage_maturing.rename(columns={'percentage': 'percentage_maturing'}, inplace=True)

    total_sti_mv = daily_mv_df.groupby(by=['the_date'])['market_value'].sum()


    total_sti_mv = df(total_sti_mv)

    final_df = pd.concat([total_sti_mv,holdings,weight_yield_df,weight_mat_df,average_mat_group,longest_maturity,percentage_maturing], axis=1)

    final_df.loc[:,'market_value'] = final_df['market_value'] / 1000000
    
    final_df.reset_index(inplace=True) 

    final_df['Type'] = 'STI Portfolio'

    final_df = final_df[['the_date','Type','market_value','number_of_holdings','weight_yield','weight_maturity','average_maturity','longest_maturity','percentage_maturing']].copy()

    final_df.loc[:,'number_of_holdings'] = final_df.loc[:,'number_of_holdings'].astype(int)

    upload_to_db(conn,final_df,table_name)


def process_daily_data(conn):

    issuer_df = created_df_from_postgres(conn,db_name='the_zoo.sti_issuers')
    daily_mv_df = created_df_from_postgres(conn,db_name='the_zoo.sti_daily_mv')
    ticker_information_df = created_df_from_postgres(conn,db_name='the_zoo.sti_ticker_information')
    daily_money_mkt_yield = created_df_from_postgres(conn,db_name='the_zoo.sti_money_mkt_yield')
    daily_purchases = created_df_from_postgres(conn,db_name='the_zoo.sti_daily_purchases')

    daily_purchases.drop('id', axis=1, inplace=True)

    daily_mv_df = get_positions_percentage(daily_mv_df,index=['the_date','ticker'])

    daily_mv_df = join_type_view_df(daily_mv_df,ticker_information_df,issuer_df)
 
    daily_mv_df.drop('id', axis=1, inplace=True)


    temp_map = daily_money_mkt_yield.set_index('the_date')['yield']

    temp_df = daily_mv_df.loc[daily_mv_df.loc[:,'issuer_name'] == 'Cash','the_date'].map(temp_map).ffill()


    daily_mv_df.loc[daily_mv_df.loc[:,'issuer_name'] == 'Cash', 'annualized_yield'] = temp_df.values / 100

    grouped = daily_mv_df.groupby('the_date')

    print daily_mv_df
    
    temp_df = df(grouped)
    #print temp_df
    #print daily_mv_df

    return daily_mv_df, ticker_information_df

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

def portfolio_guidelines(conn,daily_mv_df,table_name):

    sti_balance_view = daily_mv_df.copy()
    aggregate_limits = daily_mv_df.copy()

    #aggregate_limits.loc[aggregate_limits.loc[:,'security_sector'] == 'Government'] =  'Canada'


    #print aggregate_limits

    criteria_1 = 'opg'
    criteria_1 = "'"+criteria_1+"'"
  

    sql = ("SELECT * FROM %s WHERE client_id = %s " % (table_name,criteria_1))

    conn.execute(sql)

    data_tuple =  conn.fetchall()

    date_index = [i[0] for i in data_tuple]

    columns= ['client','the_date','market_value','delta_adj_mk']

    opg_mk_value = df.from_records(data_tuple, columns=columns)

    opg_mk_value = opg_mk_value.sort('the_date')

    opg_mk_value = opg_mk_value[['the_date','market_value']].copy()

    opg_mk_value.set_index('the_date', inplace=True)

    opg_mk_value.rename(columns={'market_value':'opg'}, inplace=True)


    opg_mk_value['opg'] = opg_mk_value['opg']/1000000

    grouped = sti_balance_view.groupby(by=['the_date'])['market_value'].sum()

    
    opg_mk_value.reset_index(inplace=True)

    sti_balance_view.reset_index(inplace=True)

    sti_balance_view['the_date'] = pd.to_datetime(sti_balance_view['the_date'])

    opg_mk_value['the_date'] = pd.to_datetime(opg_mk_value['the_date'])

    sti_balance_view = df(grouped)

    sti_balance_view.rename(columns={'market_value':'sti'},inplace=True)

    sti_balance_view['sti'] = sti_balance_view['sti']/1000000

    sti_balance_view.reset_index(inplace=True)

    sti_balance_view.set_index('the_date',inplace=True)

    opg_mk_value.set_index('the_date', inplace=True)


    combined_df = df.join(opg_mk_value,sti_balance_view,how='outer')

    sti_inputs = combined_df.sti.count()

    if sti_inputs >= 45:
        sti_inputs = 45

    combined_df.fillna(value=None,method='ffill', inplace=True)

  

    combined_df['sti/opg'] = combined_df['sti']/combined_df['opg']

    combined_df = combined_df.stack()

    combined_df = combined_df.reset_index()

    num_categories = len(set(combined_df.level_1))
    combined_df = combined_df[-sti_inputs*num_categories::]


    combined_df.rename(columns={'level_1':'balance_category',0:'market_value'}, inplace=True)

    upload_to_db(conn,combined_df,table_name='the_zoo.sti_guideline_balance')


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

    the_date = max(daily_mv_df['the_date'])
    the_date = the_date.date()

    #type_view(conn,daily_mv_df, table_name = 'the_zoo.sti_daily_sector_view',group_type='security_sector')
    #type_view(conn,daily_mv_df, table_name = 'the_zoo.sti_daily_industry_view',group_type='industry')
    #type_view(conn,daily_mv_df, table_name = 'the_zoo.sti_daily_instrument_view',group_type='category')
    #maturity_bucket_view(conn, daily_mv_df,ticker_information_df, table_name = 'the_zoo.sti_daily_maturity_bucket_view')
    #top_five_holdings(conn,daily_mv_df,ticker_information_df, table_name = 'the_zoo.sti_top_five_holdings_current')
    #portfolio_characteristics(conn, daily_mv_df,ticker_information_df, table_name = 'the_zoo.sti_daily_char')
    #portfolio_guidelines(conn, daily_mv_df,table_name = 'public.faq_total_fund_value_all_dates')

    end_time = time.time()
    time_elapsed = int(end_time - start_time)
    minutes, seconds = time_elapsed // 60, time_elapsed % 60

    print("Processing time is " + str(minutes) + ":" + str(seconds).zfill(2))

main()