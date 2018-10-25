'''

    Functions to get market data from open feed
    e.g., Sina, Netease

    Copyright(C) 2018 Jin Huang
'''
import re
import requests
import pandas as pd

def get_sina_futures_daily_data(symbol):
    '''
        get daily quote data of an futures asset specified by symbol
        
        params
        ======
        symbol string contract symbol, e.g. rb0(main), rb1901
        
        return
        ======
        data pandas.dataframe        
    '''
    # specify web api url
    url='http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesDailyKLine'
    # read data via api
    session=requests.Session()
    res=session.get(url, params={'symbol':symbol})    
    # parse raw text read from web api
    pattern='(\[[\d|\-|\.|\,|\"|\s]+\]),?'
    text=re.findall(pattern,res.text)
    #construct dataframe with parsed data
    data=pd.DataFrame(columns=['open','high','low','close','volume'])
    data.index.name='date'
    for row in text:
        #parse row data pattern
        row_pattern='\"([\d|\-|\.]*)\",?'
        record=re.findall(row_pattern, row)
        #updating existing dataframe by filling data
        data.loc[record[0]]=[float(x) for x in record[1:]]
    return data