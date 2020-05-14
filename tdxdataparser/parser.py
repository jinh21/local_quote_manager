import struct
import pandas as pd

format_code = {'m': 'hhffffiii', 'd':'<iffffiif'}

def parse_tdx_file(file_path, freq='d'):
    data = None
    try:
        fmt = format_code[freq]
        with open(file_path, 'rb') as f:
            content = f.read()
            data = struct.iter_unpack(fmt, content)
    except FileNotFoundError:
        print(f"{file_path} not found.")
    except KeyError:
        print("frequency value error, should be 'd' for daily data or 'm' for min. data")
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

def tdx2csv(src, dest, freq='d'):
    result = 0
    try:
        df = struct_daily_data(parse_tdx_file(src))
        df.to_csv(dest)
    except Exception as e:
        print("error: {}".format(e))
        result = -1
    return result
    