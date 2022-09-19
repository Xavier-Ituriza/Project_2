import pandas as pd
import sqlalchemy as alch
import requests
import datetime
import time
from scipy import stats



def count_registers_symbol():
    
    conexion = f"mysql+pymysql://root:Granada??@localhost/wall_street"
    engine = alch.create_engine(conexion)
    
    def consultar(q):
        return pd.read_sql(q, engine)
    
    return consultar("SELECT COUNT(*) FROM symbol")['COUNT(*)'][0]
    
    

def count_registers_daily_trade():
    
    conexion = f"mysql+pymysql://root:Granada??@localhost/wall_street"
    engine = alch.create_engine(conexion)
    
    def consultar(q):
        return pd.read_sql(q, engine)
    
    return consultar("SELECT COUNT(*) FROM daily_trade")['COUNT(*)'][0]
    


def count_registers_daily_stats():
    
    conexion = f"mysql+pymysql://root:Granada??@localhost/wall_street"
    engine = alch.create_engine(conexion)
    
    def consultar(q):
        return pd.read_sql(q, engine)
    
    return consultar("SELECT COUNT(*) FROM daily_stats")['COUNT(*)'][0]
        


def get_tuple_of_tickers():
    
    conexion = f"mysql+pymysql://root:Granada??@localhost/wall_street"
    engine = alch.create_engine(conexion)
    
    def consultar(q):
        return pd.read_sql(q, engine)
    
    df_tk = consultar("SELECT * FROM symbol")
    tuple_df_tk = tuple(df_tk.itertuples(index=False, name=None))
     
    return tuple_df_tk


def json_to_pandas_daily_trade (j, t):
    
    df_tdts = pd.DataFrame(j['Time Series (Daily)']).T
    df_tdts.reset_index(inplace=True)
    df_tdts.rename(columns={'index':'date','1. open':'open','2. high':'high','3. low':'low','4. close':'close','5. volume':'volume'}, inplace=True)
    df_tdts['ticker'] = t
    df_tdts['open'] = df_tdts['open'].astype(float)
    df_tdts['high'] = df_tdts['high'].astype(float)
    df_tdts['low'] = df_tdts['low'].astype(float)
    df_tdts['close'] = df_tdts['close'].astype(float)
    df_tdts['volume'] = df_tdts['volume'].astype(float)
    df_tdts['date'] = df_tdts['date'].astype('datetime64[ns]')
    df_tdts['date'] = df_tdts['date'].apply(lambda x : x.date())    
    df_tdts = df_tdts[['ticker','date','open','high','low','close','volume']]
    df_tdts.sort_values(by='date', ascending=False, inplace=True)
    
    return df_tdts


def json_to_pandas_daily_sma200 (j, t):
    
    df_sma200 = pd.DataFrame(j['Technical Analysis: SMA']).T
    df_sma200.reset_index(inplace=True)
    df_sma200.rename(columns={'index':'date', 'SMA':'value'}, inplace=True)
    df_sma200['ticker'] = t
    df_sma200['function'] = 'SMA 200'
    df_sma200['date'] = df_sma200['date'].astype('datetime64[ns]')
    df_sma200['date'] = df_sma200['date'].apply(lambda x : x.date())
    df_sma200['value'] = df_sma200['value'].astype(float)
    df_sma200 = df_sma200[['ticker','function','date','value']]
    df_sma200.sort_values(by='date', ascending=False, inplace=True)

    return df_sma200

    
def json_to_pandas_daily_sma50 (j, t):
    
    df_sma50 = pd.DataFrame(j['Technical Analysis: SMA']).T
    df_sma50.reset_index(inplace=True)
    df_sma50.rename(columns={'index':'date', 'SMA':'value'}, inplace=True)
    df_sma50['ticker'] = t
    df_sma50['function'] = 'SMA 50'
    df_sma50['date'] = df_sma50['date'].astype('datetime64[ns]')
    df_sma50['date'] = df_sma50['date'].apply(lambda x : x.date())
    df_sma50['value'] = df_sma50['value'].astype(float)
    df_sma50 = df_sma50[['ticker','function','date','value']]
    df_sma50.sort_values(by='date', ascending=False, inplace=True)

    return df_sma50


def sql_update_from_pandas(sql_table, df_updater, ticker, function):
    
    conexion = f"mysql+pymysql://root:Granada??@localhost/wall_street"
    engine = alch.create_engine(conexion)
    
    def consultar(q):
        return pd.read_sql(q, engine)
    
       
    if sql_table=='daily_trade':
        last_trade = consultar(f'SELECT MAX(date) FROM daily_trade WHERE ticker="{ticker}"')
        last_trade = last_trade['MAX(date)'][0]
        if last_trade is not None:
            df_updater = df_updater[df_updater['date']>last_trade]
        df_updater.to_sql(name='daily_trade', con=engine, schema=None, if_exists='append', index=False, index_label=None, chunksize=800, method='multi')
            
    elif sql_table=='daily_stats' and function=='SMA 200':
        last_stats = consultar(f'SELECT MAX(date) FROM daily_stats WHERE (ticker="{ticker}" AND daily_stats.function="SMA 200")')
        last_stats = last_stats['MAX(date)'][0]
        if last_stats is not None:
            df_updater = df_updater[df_updater['date']>last_stats]
        df_updater.to_sql(name='daily_stats', con=engine, schema=None, if_exists='append', index=False, index_label=None, chunksize=800, method='multi')

    elif sql_table=='daily_stats' and function=='SMA 50':
        last_stats = consultar(f'SELECT MAX(date) FROM daily_stats WHERE (ticker="{ticker}" AND daily_stats.function="SMA 50")')
        last_stats = last_stats['MAX(date)'][0]
        if last_stats is not None:
            df_updater = df_updater[df_updater['date']>last_stats]
        df_updater.to_sql(name='daily_stats', con=engine, schema=None, if_exists='append', index=False, index_label=None, chunksize=800, method='multi')


def get_tuple_of_ticker_correlations_SMA50(t):

    conexion = f"mysql+pymysql://root:Granada??@localhost/wall_street"
    engine = alch.create_engine(conexion)
    
    def consultar(q):
        return pd.read_sql(q, engine)
    
    tickers_to_correlate = consultar(f'SELECT DISTINCT daily_stats.ticker, symbol.description, MIN(date) FROM daily_stats INNER JOIN symbol ON daily_stats.ticker=symbol.ticker WHERE date<"2016-01-01" GROUP BY ticker ORDER BY ticker ASC')
    list_tickers_to_correlate = tickers_to_correlate['ticker'].tolist()
    
    df_correlations_results = pd.DataFrame(columns=['ticker','description','correlation'])
    
    list_to_correlate_SMA50_start_date = []
    list_to_correlate_SMA50_end_date = []

    for item in list_tickers_to_correlate:
    
        list_to_correlate_SMA50_start_date.append(consultar(f'SELECT MIN(date) FROM daily_stats WHERE ticker="{item}" AND daily_stats.function="SMA 50"')['MIN(date)'][0])
        list_to_correlate_SMA50_end_date.append(consultar(f'SELECT MAX(date) FROM daily_stats WHERE ticker="{item}" AND daily_stats.function="SMA 50"')['MAX(date)'][0])
    
    values_to_correlate_SMA50 = {}

    for item in list_tickers_to_correlate:
    
        values_to_correlate_SMA50.update({item : consultar(f'SELECT date, value FROM daily_stats WHERE ticker="{item}" AND daily_stats.function="SMA 50" AND date BETWEEN "{max(list_to_correlate_SMA50_start_date)}" AND "{min(list_to_correlate_SMA50_end_date)}" ORDER BY date DESC')})
    
    
    for item in list_tickers_to_correlate:
        
        if item != t:
            corr = stats.spearmanr(values_to_correlate_SMA50[t]['value'], values_to_correlate_SMA50[item]['value'])[0]
            df_correlations_results = df_correlations_results.append({'ticker':item, 'description':tickers_to_correlate[tickers_to_correlate['ticker']==item]['description'].tolist()[0], 'correlation':corr}, ignore_index=True)

    df_correlations_results.sort_values(by=['correlation'], inplace=True, ascending=False)
    
    tuple_df_correlations_results = tuple(df_correlations_results.itertuples(index=False, name=None))
           
    return tuple_df_correlations_results, df_correlations_results