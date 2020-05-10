# -*- coding: utf-8 -*-
"""
Created on Wed Jul  4 15:21:43 2018

@author: jinh21
"""
import pandas as pd

import sqlalchemy

from gevent.pool import Pool

#tushare library
import tushare as tsdata

# progress bar
from tools import update_progress


class Dataset(object):

    def __init__(self, conn_str):
        try:
            self.dbengine = sqlalchemy.create_engine(conn_str)
        except Exception:
            print(f'{conn_str} is wrong.')

    def connect(self):
        return self.dbengine.connect()

    def getColumn(self, tbl_name, field_name):
        with self.connect() as conn:
            sql = f'select {field_name} from {tbl_name}'
            data = pd.read_sql(sql, con=conn)
        return data

    def getTable(self, tbl_name):
        with self.connect() as conn:
            data = pd.read_sql_table(tbl_name, conn)
        return data

    def getMaxValue(self, tbl_name, field_name):
        sql = f'select max({field_name}) from {tbl_name}'
        result = self.execute(sql)
        return result[0][0]

    def execute(self, sql):
        with self.connect() as conn:
            result = conn.execute(sql)
            data = result.fetchall()
        return data

    def dropTable(self, tbl_name):
        sql = f'drop table {tbl_name}'
        self.execute(sql)

    def saveDataAsTable(self, tbl_name, data,
                        if_exists=None, index_label=None):
        with self.connect() as conn:
            try:
                data.to_sql(
                    tbl_name, con=conn,
                    if_exists=if_exists,
                    index_label=index_label
                    )
            except sqlalchemy.exc.OperationalError:
                pass


class StockData(object):
    conn_str = "sqlite:///data.db"
    tbl_name_symbols = 'stock_info'

    def __init__(self, conn_str=None):
        self.logfile = "data/log.txt"
        if not conn_str:
            self.dataset = Dataset(self.conn_str)
        self.updated_data = {}

    def getStockInfoViaTushare(self):
        stocks_basics_info = tsdata.get_stock_basics()
        return stocks_basics_info

    def getStockDataViaTushare(self, symbol, start=None, end=None):
        if start:
            start = pd.to_datetime(start).strftime("%Y-%m-%d")
        if end:
            end = pd.to_datetime(start).strftime("%Y-%m-%d")
        if not start:
            start = '1900-01-01'
        return tsdata.get_k_data(symbol, start, end)

    def getLocalStockInfo(self):
        return self.dataset.getTable(self.tbl_name_symbols)    

    def getLocalStockData(self, symbol, start=None, end=None):
        tbl_name = self.getTableName(symbol)
        if start:
            start = pd.to_datetime(start).strftime("%Y-%m-%d")
        if end:
            end = pd.to_datetime(start).strftime("%Y-%m-%d")
        if not start:
            start = '1900-01-01'
        sql = (
            f'select date, open, close, high,'
            f' low, volume from {tbl_name} where date > "{start}"'
            )
        if end:
            sql += ' and date < "{end}"'

        try:
            data = self.dataset.execute(sql)
            df = pd.DataFrame(
                data=data,
                columns=['date', 'open', 'close', 'high', 'low', 'volume']
                )
            result = df.set_index('date')
        except sqlalchemy.exc.OperationalError:
            result = pd.DataFrame(
                columns=['date', 'open', 'close', 'high', 'low', 'volume']
                )
        return result

    def getStockSymbols(self):
        try:
            data = self.dataset.getColumn(self.tbl_name_symbols, 'symbol')
            symbols = data['symbol']
        except sqlalchemy.exc.OperationalError:
            info = self.updateStockInfo()
            symbols = info['symbol']
        return symbols

    def getTableName(self, symbol):
        return f'stock_{symbol}'

    def getUpdatedStockData(self, symbol):
        tbl_name = self.getTableName(symbol)
        try:
            last_update_date = self.dataset.getMaxValue(tbl_name, 'date')
        except Exception:
            last_update_date = '1970-01-01'
        data = self.getStockDataViaTushare(symbol, start=last_update_date)
        return data

    def updateStock(self, symbol, data):
        try:
            self.dataset.saveDataAsTable(
                self.getTableName(symbol),
                data=data,
                if_exists='append',
                )
        except sqlalchemy.exc.OperationalError:
            print('error: failed to update stock.')

    def updateStockInfo(self):
        info = self.getStockInfoViaTushare()
        self.dataset.saveDataAsTable(
            self.tbl_name_symbols,
            info,
            if_exists='replace', index_label='symbol')
        return info

    def saveStock(self, symbol, data):
        tbl_name = self.getTableName(symbol)
        self.dataset.saveDataAsTable(tbl_name, data=data, if_exists='append')

    def log(self, msg, display=False):
        if display:
            print(f"{pd.to_datetime('now')}:{msg}")
        with open(self.__log_file, "a+") as f:
            f.write(f"{pd.to_datetime('now')}:{msg}\r\n")


class StockUpdater(object):

    def __init__(self, conn_str=None):
        self.data = StockData(conn_str)
        self.step = 1
        self.total = 0

    def getAllUpdatedStockData(self, workers=5):
        pool = Pool(workers)
        symbols = self.data.getStockSymbols()
        self.total = len(symbols)
        results = pool.map(self.getUpdatedStockData, symbols)
        return results

    def getUpdatedStockData(self, symbol):
        data = self.data.getUpdatedStockData(symbol)
        update_progress(self.step/self.total)
        self.step += 1
        return data

    def updateStocks(self, data):
        step = 1
        total = len(data)
        error_stocks = []
        for d in data:
            update_progress(step/total)
            if not d.empty:
                symbol = d['code'].iloc[0]
                try:
                    self.data.saveStock(symbol, d)
                except sqlalchemy.exc.OperationalError:
                    error_stocks.append(symbol)
            step += 1
        return error_stocks


if __name__ == '__main__':
    updater = StockUpdater()
    print('retrieving data ...')
    results = updater.getAllUpdatedStockData()
    print('\nupdating database ...')
    errolist = updater.updateStocks(results)
