import functools
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import akshare as ak
import duckdb
import pandas as pd
from retry import retry


def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Function '{func.__name__}' executed in {elapsed_time:.2f} seconds", file=sys.stderr)
        return result

    return wrapper


@timeit
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


@timeit
def is_closing_price_limit_up(df):
    """
    Add new column: is_closing_price_limit_up
    """
    query = """
        select *,
               if(ABS(涨跌幅 - if(股票代码 like '3%', 20, 10)) < 0.5, true, false) as 是否收盘涨停
        from df
    """
    return duckdb.sql(query).df()


@timeit
def consecutive_limit_up_days(df):
    """
    Add new column: consecutive_limit_up_days
    """
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


def calculate_avg_premium_rate(df, n, date):
    date = pd.to_datetime(date)

    # Filter stocks with 连续收盘涨停天数 = n on the given date
    target_stocks = df[(df['连续收盘涨停天数'] == n) & (df['日期'] == date)]

    if target_stocks.empty:
        return None

    # Find the next trading day
    df_sorted = df.sort_values(by='日期')
    unique_dates = df_sorted['日期'].unique()
    try:
        next_day_index = list(unique_dates).index(date) + 1
        next_trading_day = unique_dates[next_day_index]
    except (IndexError, ValueError):
        return None

    # Merge the target stocks with the next trading day's data
    next_day_data = df_sorted[df_sorted['日期'] == next_trading_day]
    if next_day_data.empty:
        return None

    merged_df = pd.merge(target_stocks, next_day_data, on='股票代码', suffixes=('', '_next'))

    # Calculate 溢价率 as (Next day's 开盘价 - Current day's 收盘价) / Current day's 收盘价
    merged_df['溢价率'] = (merged_df['开盘_next'] - merged_df['收盘']) / merged_df['收盘']

    # Calculate the average 溢价率
    avg_premium_rate = merged_df['溢价率'].mean()
    if avg_premium_rate is not None:
        avg_premium_rate = f"{avg_premium_rate:.2%}"
    return avg_premium_rate


def calculate_positive_premium_rate(df, n, date):
    date = pd.to_datetime(date)

    # Filter stocks with 连续收盘涨停天数 = n on the given date
    target_stocks = df[(df['连续收盘涨停天数'] == n) & (df['日期'] == date)]
    if target_stocks.empty:
        return None

    # Find the next trading day
    df_sorted = df.sort_values(by='日期')
    unique_dates = df_sorted['日期'].unique()
    try:
        next_day_index = list(unique_dates).index(date) + 1
        next_trading_day = unique_dates[next_day_index]
    except (IndexError, ValueError):
        return None

    # Merge the target stocks with the next trading day's data
    next_day_data = df_sorted[df_sorted['日期'] == next_trading_day]
    if next_day_data.empty:
        return None

    merged_df = pd.merge(target_stocks, next_day_data, on='股票代码', suffixes=('', '_next'))

    # Calculate the premium ratio as (Next day's 开盘价 - Current day's 收盘价) / Current day's 收盘价
    merged_df['溢价率'] = (merged_df['开盘_next'] - merged_df['收盘']) / merged_df['收盘']

    # Calculate the positive premium ratio
    positive_premium_count = (merged_df['溢价率'] > 0).sum()
    total_count = len(merged_df)

    if total_count == 0:
        return None

    positive_premium_rate = positive_premium_count / total_count
    positive_premium_rate = f"{positive_premium_rate:.2%}"
    return positive_premium_rate


@timeit
def generate_summary_dataframe(df, start_date, end_date):
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Get the unique dates in the DataFrame
    unique_dates = df['日期'].unique()
    date_range = pd.date_range(start=start_date, end=end_date)
    summary_data = []

    for date in date_range:
        if date not in unique_dates:
            continue  # Skip non-trading days

        row = {'日期': date}

        for n in range(1, 9):  # For 连续收盘涨停天数 from 1 to 8
            stock_count = df[(df['连续收盘涨停天数'] == n) & (df['日期'] == date)].shape[0]
            avg_premium_rate = calculate_avg_premium_rate(df, n, date)
            positive_premium_rate = calculate_positive_premium_rate(df, n, date)

            row[f'连涨{n}天股票数'] = stock_count
            row[f'连涨{n}天溢价率'] = avg_premium_rate
            row[f'连涨{n}天胜率'] = positive_premium_rate

        summary_data.append(row)

    summary_df = pd.DataFrame(summary_data).sort_values(by='日期', ascending=False)
    return summary_df
