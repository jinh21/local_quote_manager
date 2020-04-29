"""



    Copyright (c) Kean. All rights reserved.
    Licensed under the Apache License v2.0..



    created: 2020-04-29 10:36:41


"""

import struct
import pandas as pd

format_code = {'m': 'hhffffiii', 'd':'<iffffiif'}

def parse_tdx_file(file_path, data_freq='d'):
    data = None
    try:
        fmt = format_code[data_freq]
        with open(file_path, 'rb') as f:
            content = f.read()
            data = struct.iter_unpack(fmt, content)
    except FileNotFoundError:
        print(f"{file_path} not found.")
    except KeyError:
        print("data_freq value error, should be 'd' for daily data or 'm' for min. data")
    return data


def struct_daily_data(data):
    content = []
    for d in data:
        content.append(d)
    df = pd.DataFrame(data=content, columns=['date', 'open','high', 'low','close', 'oi','volume','settle'])
    df.set_index('date', inplace=True)
    df = pd.DataFrame(data=content, columns=['date', 'open','high', 'low','close', 'oi','volume','settle'])
    df.set_index('date',inplace=True)
    df.index = pd.to_datetime(df.index, format='%Y%m%d')
    return df



def get_tdx_min_fut_data(file_path):
    data = None

    return data

def get_tdx_daily_fut_data(file_path):
    data = parse_tdx_file(file_path, 'd')

    return data