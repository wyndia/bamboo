import akshare as ak
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
