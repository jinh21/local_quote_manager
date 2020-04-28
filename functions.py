'''

    Functions to get market data from open feed
    e.g., Sina

    Copyright(C) 2020 jinh21
'''
import re
import requests
import pandas as pd


base_url = "http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesDailyKLine"


def get_futures_daily_data_from_sina(symbol):
    '''
        get daily quote data of an futures asset specified by symbol

        params
        ======
        symbol string
            contract symbol, e.g. rb0(main), rb1901

        return
        ======
        data pandas.dataframe
            the data of daily K bar
    '''
    # read data via api
    session = requests.Session()
    res = session.get(base_url, params={'symbol': symbol})
    # parse raw text read from web api
    pattern = r'(\[[\d|\-|\.|\,|\"|\s]+\]),?'
    text = re.findall(pattern, res.text)
    # construct dataframe with parsed data
    data = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
    data.index.name = 'date'
    for row in text:
        # parse row data pattern
        row_pattern = r'\"([\d|\-|\.]*)\",?'
        record = re.findall(row_pattern, row)
        # updating existing dataframe by filling data
        data.loc[record[0]] = [float(x) for x in record[1:]]
    return data
