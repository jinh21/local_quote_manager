"""



   Copyright (c) Kean Huang. All rights reserved.
   Licensed under the MIT License.



   created: 2020-04-27 16:00:18


"""
import json
import sqlalchemy as sc
from sqlalchemy.ext.declarative import declarative_base
import importlib

Base = declarative_base()

def check_pacakge_existence(pkg_name):
    spec = importlib.util.find_spec(pkg_name)
    found = spec is not None
    return found

# load the content of a configuration file
# specified by a given file path
def load_configure(file_path="sysconf.json"):
    conf = {}
    with open(file_path, 'r') as f:
        conf = json.load(f)
    return conf


# create the connection string
# based on the configuration
def create_conn_str(conf):
    db_type = conf['db']['type']
    db_driver = conf['db']['driver']
    db_username = conf['db']['username']
    db_password = conf['db']['password']
    db_host = conf['db']['host']
    db_port = conf['db']['port']
    db_name = conf['db']['name']

    conn_str = (f"{db_type}+{db_driver}://"
                f"{db_username}:{db_password}"
                f"@{db_host}:{db_port}/{db_name}")
    return conn_str


def create_engine(conn_str, echo=False):
    engine = sc.create_engine(conn_str, echo=echo)
    return engine


def create_session(engine):
    Session = sc.orm.sessionmaker(bind=engine)
    session = Session()
    return session

class SinaKLines(object):

    idx = sc.Column(sc.Integer, primary_key=True)
    date = sc.Column(sc.DateTime)
    symbol = sc.Column(sc.String)
    open = sc.Column(sc.Float)
    high = sc.Column(sc.Float)
    low = sc.Column(sc.Float)
    close = sc.Column(sc.Float)
    volume = sc.Column(sc.Float)


class SinaFutures1dKlines(Base, SinaKLines):
    __tablename__ = 'sinafutures1dklines'


class SinaFutures60mKlines(Base, SinaKLines):
    __tablename__ = 'sinafutures60mklines'


class SinaFutures30mKlines(Base, SinaKLines):
    __tablename__ = 'sinafutures30mklines'


class SinaFutures15mKlines(Base, SinaKLines):
    __tablename__ = 'sinafutures15mklines'


class SinaFutures5mKlines(Base, SinaKLines):
    __tablename__ = 'sinafutures5mklines'
    


def create_tables(engine):
    result = Base.metadata.create_all(engine)
    return result



def sina_kline_cls_factory(freq='1d'):
    kline_cls = SinaFutures1dKlines
    if freq == '60m':
        kline_cls = SinaFutures60mKlines
    elif freq == '30m':
        kline_cls = SinaFutures30mKlines
    elif freq == '15m':
        kline_cls = SinaFutures15mKlines
    elif freq == '5m':
        kline_cls = SinaFutures5mKlines
    return kline_cls


def check_existence(engine, tblname):
    existence = engine.dialect.has_table(engine, tblname)
    return existence


def add_sina_futures_kline(session, date, symbol,
                           open, high, low, close,
                           volume, freq='1d'):
    result = 0
    try:
        kline_cls = sina_kline_cls_factory(freq)        
        kline = kline_cls(
            date=date, symbol=symbol,
            open=open, high=high, low=low, close=close,
            volume=volume
            )
        session.add(kline)
        session.commit()
    except Exception:
        result = -1
    return result
