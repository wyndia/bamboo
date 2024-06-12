import akshare as ak
import duckdb
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from retry import retry


def fetch_stock_data_parallel(stock_list, start_date, end_date, adjust="qfq", max_workers=100):
    @retry(tries=3, delay=2, backoff=2)
    def fetch_single_stock_data(symbol):
        stock_data = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date,
                                        adjust=adjust)
        stock_data['股票代码'] = symbol
        return stock_data

    all_data = pd.DataFrame()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(fetch_single_stock_data, symbol): symbol for symbol in stock_list}
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                data = future.result()
                all_data = pd.concat([all_data, data], ignore_index=True)
            except Exception as e:
                print(f"Error processing data for {symbol}: {e}")
    return all_data


def is_closing_price_limit_up(df):
    query = """
        select *,
               if(ABS(涨跌幅 - if(股票代码 like '3%', 20, 10)) < 0.5, true, false) as 是否收盘涨停
        from df
    """
    return duckdb.sql(query).df()


def consecutive_limit_up_days(df):
    df = df.sort_values(by=['股票代码', '日期'])
    df['连续收盘涨停天数'] = 0
    for code in df['股票代码'].unique():
        stock_df = df[df['股票代码'] == code]
        streak = 0
        for i, row in stock_df.iterrows():
            if row['是否收盘涨停']:
                streak += 1
            else:
                streak = 0
            df.at[i, '连续收盘涨停天数'] = streak
    return df
