import pandas as pd

from airflow.decorators import dag, task
from datetime import datetime, timedelta

from Analysis import Analysis
from Connection import Connection
from stock_price import download_stock_data, insert_stock_data
from stock_news import download_stock_news, process_news, insert_stock_news
from stock_tweet import extract_tweets, process_tweets, insert_stock_tweets

# Connection ID for the database. Make sure connection is set up in Airflow UI
CONN_ID = '' # TO BE SET

# Set how many hours of data to extract, in hours
PERIOD_STR = '8h'
PERIOD = 8

default_args = {
    "owner": "is3107_grp24",
    "start_date": datetime(2024, 1, 1), # Set the start date of the DAG to January 1st, 2024
    "email": ["chongwaisheen678@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    # If a task fails, retry it thrice after 10 minutes
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

@dag(
    dag_id='stock_tweet_headline', 
    default_args=default_args, 
    schedule_interval='0 0,8,16 * * *',  # Schedule to run at midnight, 8 AM, and 4 PM (every 8 hours)
    catchup=False, 
    tags=['is3107']
)
def stock_tweet_headline_etlt():
    @task
    def initialise_db(connection: Connection, conn_id: str) -> bool:
        ''' Connects to the database and initialises the tables. Returns True if successful, False otherwise. '''
        connection.connect(conn_id=conn_id)
        connection.init_db()
        return CONN_ID

    ###################### EXTRACT stock prices, news, tweets from data sources ######################
    @task
    def extract_stock_price(stocks: list, period: str, interval: str) -> dict:
        ''' Loads the stock prices of the stock tickers passed in as arguments from Yahoo Finance. '''
        stock_data = download_stock_data(stocks=stocks, period=period, interval=interval)
        return stock_data
    
    @task
    def extract_stock_news(stocks: list) -> dict:
        ''' Extract stock news headlines '''
        news = download_stock_news(stocks)
        return news
    
    @task
    def extract_stock_tweets(stocks: list, period: int) -> pd.DataFrame:
        ''' Extract stock tweets from google drive, which contains the twitter data. Period refers to the number of hours. '''
        tweets = extract_tweets(stocks, period=period)
        return tweets
    
    ###################### TRANSFORM stock prices, news, tweets ######################
    @task
    def transform_stock_news(news: dict, period: int, conn_id: str) -> pd.DataFrame:
        ''' Filter the headlines and perform sentiment analysis. Period refers to the number of hours. '''
        df = process_news(news, period, conn_id)
        return df
    
    @task
    def transform_stock_tweets(tweets: pd.DataFrame, conn_id: str) -> pd.DataFrame:
        ''' Process the tweets and perform sentiment analysis '''
        df = process_tweets(tweets, conn_id)
        return df
    
    ###################### LOAD stock prices, news, tweets into data warehouse ######################
    @task
    def load_stock_price(stock_data: dict, conn_id: str) -> bool:
        ''' Load the stock prices into the database '''
        try: 
            insert_stock_data(stock_data=stock_data, conn_id=conn_id)
            return True
        except Exception as e:
            print(f'Error loading stock data: {e}')
            return False
        
    @task
    def load_stock_news(news_with_sentiments: pd.DataFrame, conn_id: str) -> bool:
        ''' Load the stock news with sentiment analyses into the database '''
        try:
            insert_stock_news(news_with_sentiments, conn_id=conn_id)
            return True
        except Exception as e:
            print(f'Error loading stock news: {e}')
            return False
        
    @task
    def load_stock_tweets(tweets_with_sentiments: pd.DataFrame, conn_id: str) -> bool:
        ''' Load the tweets with sentiment analyses into the database '''
        try:
            insert_stock_tweets(tweets_with_sentiments, conn_id=conn_id)
            return True
        except Exception as e:
            print(f'Error loading stock tweets: {e}')
            return False
        
    ###################### TRANSFORM for analyses ######################
    @task
    def transform_for_prediction(prices: bool, news: bool, tweets: bool, analysis: Analysis, stocks_list: list) -> pd.DataFrame:
        ''' Perform querying on price, news and tweets tables. 
        Then performs the transformation required (e.g. calculating moving averages) for time series forecasting. 
        '''
        if prices and news and tweets:
            transformed_data = {}

            for company in stocks_list:
                print(f"Processing data for {company}")
                features_df, target_df = analysis.process_moving_averages_for_stock(company)
                transformed_data[company] = {
                    'features': features_df,
                    'target': target_df
                }
            
            return transformed_data
        else:
            print(prices, news, tweets)
            print("Error in loading data. Exiting...")
            return {}

    @task
    def generate_predictions(analysis: Analysis, transformed_data: dict) -> pd.DataFrame:
        ''' With the transformed data, train the LSTM model and conduct stock price predictions.'''
        predictions = analysis.train_and_predict(transformed_data)
        return predictions
    
    ###################### LOAD the resulting predictions ######################
    @task
    def load_predictions(analysis: Analysis, predictions: pd.DataFrame):
        ''' Load the predictions back into google sheets. '''
        analysis.store_results(predictions, 'lstm_moving_average_predictions')

    ###################### START OF DAG WORKFLOW ######################
    stocks_list = [
        'TSLA', 'MSFT', 'PG', 'META', 'AMZN', 'GOOG', 'AMD', 'AAPL', 'NFLX', 'TSM', 'KO', 'F', 'COST', 'DIS', 'VZ', 'CRM', 'INTC', 
        'BA', 'BX', 'NOC', 'PYPL', 'ENPH', 'NIO', 'ZS', 'XPEV'
    ]

    # Initialise the database
    db_connection = Connection()
    connection_id = initialise_db(db_connection, conn_id=CONN_ID)

    # Extract and load stock price data
    stock_prices = extract_stock_price(stocks=stocks_list, period=PERIOD_STR, interval='1h')
    load_prices_success = load_stock_price(stock_data=stock_prices, conn_id=connection_id)
    
    # Extract, transform and load stock news data
    news = extract_stock_news(stocks=stocks_list)
    transformed_news = transform_stock_news(news=news, period=PERIOD, conn_id=connection_id) # Note that maximum number of days is 100 (2,400 hours)
    load_news_success = load_stock_news(news_with_sentiments=transformed_news, conn_id=CONN_ID)

    # Extract, transform and load stock tweet data
    tweets = extract_stock_tweets(stocks=stocks_list, period=PERIOD)
    transformed_tweets = transform_stock_tweets(tweets=tweets, conn_id=connection_id)
    load_tweets_success = load_stock_tweets(tweets_with_sentiments=transformed_tweets, conn_id=CONN_ID)
    
    # Perform further transformations for analysis
    analysis_obj = Analysis(conn_id=CONN_ID)
    transformed_data = transform_for_prediction(prices=load_prices_success, news=load_news_success, tweets=load_tweets_success,
                                                 analysis=analysis_obj, stocks_list=stocks_list)
    predictions = generate_predictions(analysis=analysis_obj, transformed_data=transformed_data)

    # Load and store the predictions
    load_predictions(analysis=analysis_obj, predictions=predictions)

project_dag = stock_tweet_headline_etlt()