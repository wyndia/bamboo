import pytz
from datetime import datetime, timedelta
from utils import *


def fetch_recent_data_on_the_fly(days=90):
    """
    Temporary solution. Will read data from persisted data in the future.
    """
    tz_utc_8 = pytz.timezone('Asia/Shanghai')
    end_date = datetime.now(tz_utc_8).strftime('%Y%m%d')
    start_date = (datetime.today() - timedelta(days)).strftime('%Y%m%d')

    stock_data = ak.stock_zh_a_spot_em()
    sh_stock_list = stock_data.query('代码.str.startswith("6")')['代码'].tolist()
    sz_stock_list = stock_data.query('代码.str.startswith("0")')['代码'].tolist()
    cyb_stock_list = stock_data.query('代码.str.startswith("3")')['代码'].tolist()
    stock_list = sh_stock_list + sz_stock_list + cyb_stock_list

    result = fetch_stock_data_parallel(stock_list, start_date, end_date, 'qfq')
    result['日期'] = pd.to_datetime(result['日期'], errors='coerce')
    result['股票代码'] = result['股票代码'].astype('string')
    return result


if __name__ == '__main__':
    all_stock_data = fetch_recent_data_on_the_fly()
    all_stock_data = is_closing_price_limit_up(all_stock_data)
    all_stock_data = consecutive_limit_up_days(all_stock_data)
    print(all_stock_data)
