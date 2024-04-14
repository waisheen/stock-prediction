import pandas as pd
import psycopg2

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from psycopg2.extensions import connection, cursor

from stock_db import Connection
from stock_price import download_stock_data, insert_stock_data
from stock_news import download_stock_news, process_news, insert_stock_news
from stock_tweet import extract_tweets, process_tweets, insert_stock_tweets

default_args = {
    "owner": "is3107_grp24",
    "start_date": datetime(2024, 1, 1), # Set the start date of the DAG to January 1st, 2024
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # "retry_delay": timedelta(minutes=10)
}

@dag(
    dag_id='stock_tweet_headline', 
    default_args=default_args, 
    schedule=timedelta(minutes=5), 
    catchup=False, 
    tags=['is3107']
)
def stock_tweet_headline_etlt():
    @task
    # def initialise_db(connection: Connection, dbname: str, user: str, password: str, host: str, port: int):
    def initialise_db(connection: Connection, conn_id: str):
        ''' Connects to the database and initialises the tables. Returns True if successful, False otherwise. '''
        try: 
            # Connect and initialise the database
            connection.connect(conn_id=conn_id)
            connection.init_db()
            return True
            # return connection.cur
        except Exception as e:
            print(f'Error initialising database: {e}')
            return False

    @task
    def extract_stock_price(stocks: list) -> dict:
        ''' Loads the stock prices of the stock tickers passed in as arguments from Yahoo Finance. '''
        stock_price_data = download_stock_data(stocks)
        return stock_price_data
    
    @task
    def load_stock_price(stock_data: dict, conn: Connection) -> bool:
        ''' Load the stock prices into the database '''
        try: 
            with conn.cursor() as cur:
                insert_stock_data(stock_data, cur)
            conn.conn.commit()
            return True
        except Exception as e:
            print(f'Error loading stock data: {e}')
            return False
        
    # @task
    # def extract_stock_news(stocks: list) -> pd.DataFrame:
    #     ''' Extract stock news headlines '''
    #     news = download_stock_news(stocks)
    #     return news
    
    # @task
    # def transform_stock_news(news: pd.DataFrame, period: int=1) -> pd.DataFrame:
    #     ''' Filter the headlines and perform sentiment analysis '''
    #     df = process_news(news, period)
    #     return df
    
    # @task
    # def load_stock_news(news_with_sentiments: pd.DataFrame, conn: connection) -> bool:
    #     ''' Load the stock news with sentiment analyses into the database '''
    #     try:
    #         with conn.cursor() as cur:
    #             insert_stock_news(news_with_sentiments, cur)
    #         conn.commit()
    #         return True
    #     except Exception as e:
    #         print(f'Error loading stock news: {e}')
    #         return False
        
    # @task
    # def extract_stock_tweets(stocks: list, period: int) -> pd.DataFrame:
    #     ''' Extract stock tweets from google drive, which contains the twitter data'''
    #     tweets = extract_tweets(stocks, period=period)
    #     return tweets
    
    # @task
    # def transform_stock_tweets(tweets: pd.DataFrame) -> pd.DataFrame:
    #     ''' Process the tweets and perform sentiment analysis '''
    #     df = process_tweets(tweets)
    #     return df
    
    # @task
    # def load_stock_tweets(tweets_with_sentiments: pd.DataFrame, conn: connection) -> bool:
    #     ''' Load the tweets with sentiment analyses into the database '''
    #     try:
    #         with conn.cursor() as cur:
    #             insert_stock_tweets(tweets_with_sentiments, cur)
    #         conn.commit()
    #         return True
    #     except Exception as e:
    #         print(f'Error loading stock tweets: {e}')
    #         return False
    

    # Flow of the DAG
    # params = {
    #     'dbname': 'is3107',
    #     'user': 'is3107',
    #     'password': 'is3107',
    #     # 'host': 'host.docker.internal',
    #     'host': 'localhost',
    #     'port': 5433
    # }
    stocks_list = ['AMZN', 'TSLA', 'NVDA', 'AAPL', 'MSFT', 'META']

    # Initialise the database
    db_connection = Connection()
    connection_success = initialise_db(db_connection, conn_id='proj')

    if connection_success:
        # Extract and load stock price data
        stock_data = extract_stock_price(stocks=stocks_list)
        load_success = load_stock_price(stock_data=stock_data, conn=db_connection) # airflow shows success, but data does not get inserted

        # Extract, transform and load stock news data
        # news = extract_stock_news(stocks=stocks_list)
        # news_with_sentiments = transform_stock_news(news=news, period=1)
        # load_news_success = load_stock_news(news_with_sentiments=news_with_sentiments, conn=conn)

        # # Extract, transform and load stock tweet data
        # tweets = extract_stock_tweets(stocks=stocks_list, period=1)
        # tweets_with_sentiments = transform_stock_tweets(tweets=tweets)
        # load_tweets_success = load_stock_tweets(tweets_with_sentiments=tweets_with_sentiments, conn=conn)
    
    # Perform transformation etc.


project_dag = stock_tweet_headline_etlt()