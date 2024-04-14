import argparse
import yfinance as yf
import pandas as pd
import psycopg2
import sys

from psycopg2.extensions import cursor

def download_stock_data(stocks: list, period: str='5m', interval: str='5m') -> dict:
    ''' Download (extract) stock data from Yahoo Finance'''
    
    date_format = "%b-%d-%y %H:%M"
    stock_data = {}

    for stock in stocks:
        df = yf.download(stock, period=period, interval=interval)
        
        if not df.empty:
            df.index = df.index.strftime(date_format)
            df.reset_index(inplace=True)
        
        stock_data[stock] = df

    return stock_data


def get_stock_info(ticker_symbol: str) -> dict:
    ''' Get stock information from Yahoo Finance from the ticker symbol'''

    stock = yf.Ticker(ticker_symbol)
    
    try:
        name = stock.info['longName']
        sector = stock.info['sector']
        return {'Name': name, 'Sector': sector}
    except KeyError as e:
        print(f"Could not find '{e.args[0]}' information for ticker symbol {ticker_symbol}")
    
    
def get_or_create_company_id(ticker_symbol: str, cur: cursor) -> int:
    ''' Get the company_id if it exists, add the company_id and its information into the company table if not'''

    cur.execute("SELECT company_id FROM company WHERE ticker_symbol = %s", (ticker_symbol,))
    result = cur.fetchone()
    
    if result:
        return result[0]
    else:
        company_info = get_stock_info(ticker_symbol)
        cur.execute("INSERT INTO company (company_name, ticker_symbol, sector) VALUES (%s, %s, %s) RETURNING company_id",
                    (company_info['Name'], ticker_symbol, company_info['Sector']))
        company_id = cur.fetchone()[0]
        return company_id
    
    
def insert_stock_data(stock_data: dict, cur: cursor) -> None:
    ''' Load stock data into the price table in the database'''

    insert_query = """
    INSERT INTO price (company_id, date, open_price, close_price, high_price, low_price, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    
    for stock, df in stock_data.items():
        try:
            for _, row in df.iterrows():
                company_id = get_or_create_company_id(stock, cur)
                
                data_tuple = (
                    company_id,
                    pd.to_datetime(row['Datetime'], format="%b-%d-%y %H:%M"),
                    row['Open'],
                    row['Close'],
                    row['High'],
                    row['Low'],
                    row['Volume']
                )
                
                cur.execute(insert_query, data_tuple)
            
            print(f'{stock} data inserted')
        
        except Exception as e:
            print(f'Error inserting {stock}: {e}')


# if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Download stock data and insert into PostgreSQL database")
    # parser.add_argument("--stocks", nargs="+", help="List of stock tickers")
    # args = parser.parse_args()

    # if args.stocks:
    #     stocks = args.stocks
    # else:
    #     print("No stocks provided.")
    #     sys.exit()

    # stock_data = download_stock_data(stocks)

    # params = {
    #     'dbname': 'is3107',
    #     'user': 'is3107',
    #     'password': 'is3107',
    #     'host': 'localhost',
    #     'port': 5433
    # }

    # conn = psycopg2.connect(**params)
    # cur = conn.cursor()

    # insert_stock_data(stock_data, cur)

    # conn.commit()
    # conn.close()
    # print("Stock data inserted into database successfully.")
    # return stock_data