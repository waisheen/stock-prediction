from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from torch.nn.functional import softmax
import torch
import psycopg2
import argparse
import sys
from psycopg2.extensions import cursor
from airflow.providers.postgres.hooks.postgres import PostgresHook

from stock_price import get_or_create_company_id

# # Get stock info from yfinance
# def get_stock_info(ticker_symbol):
#     stock = yf.Ticker(ticker_symbol)
    
#     try:
#         name = stock.info['longName']
#         sector = stock.info['sector']
#         return {'Name': name, 'Sector': sector}
#     except KeyError as e:
#         return f"Could not find '{e.args[0]}' information for ticker symbol {ticker_symbol}"

# # Get the company_id if it exists, add the company_id if not
# def get_or_create_company_id(ticker_symbol, cur):
#     # Check if the company exists
#     cur.execute("SELECT company_id FROM company WHERE ticker_symbol = %s", (ticker_symbol,))
#     result = cur.fetchone()
    
#     if result:
#         return result  # Return existing company_id
#     else:
#         # Get company info
#         company_info = get_stock_info(ticker_symbol)
#         cur.execute("INSERT INTO company (company_name, ticker_symbol, sector) VALUES (%s, %s, %s) RETURNING company_id",
#                     (company_info['Name'], ticker_symbol, company_info['Sector']))
#         company_id = cur.fetchone()[0]
#         return company_id
    

def download_stock_news(stocks: list) -> dict:
    ''' Download stock news from Finviz'''
    news = {}
    for stock in stocks:
        url = f'https://finviz.com/quote.ashx?t={stock}&p=d'
        request = Request(url=url, headers={'user-agent': 'news_scraper'})
        response = urlopen(request)
        
        # parse the HTML content
        html = BeautifulSoup(response, features='html.parser')
        finviz_news_table = html.find(id='news-table')
        news[stock] = finviz_news_table

    return news


def parse_datetime(datetime_str: str, current_date: datetime) -> tuple:
    ''' Transform the datetime string into a standardised format'''

    if 'Today' in datetime_str or datetime_str.count('-') == 0:
        date_part = current_date.strftime('%b-%d-%y')
        time_part = datetime_str.replace('Today', '').strip()
    else:
        # Split into date and time parts for explicit dates
        date_part, time_part = datetime_str.split(' ')
        # Update the current date to this new date
        current_date = datetime.strptime(date_part, '%b-%d-%y')  # Assuming year 2024 for example purposes

    # Convert AM/PM times to 24-hour format and return standardized datetime string
    full_datetime_str = f"{date_part} {time_part}"
    full_datetime = datetime.strptime(full_datetime_str, '%b-%d-%y %I:%M%p')
    
    return full_datetime.strftime('%b-%d-%y %H:%M'), current_date

def extract_label(row):
    return row[0]['label']

def extract_score(row):
    return row[0]['score']

def process_news(news: dict, period: int) -> pd.DataFrame:
    ''' Transform the news extracted from Finviz and return a dataframe with sentiment analysis'''
    
    news_extracted = []
    current_date = datetime.now()

    for stock, news_item in news.items():
        for row in news_item.findAll('tr'):
            headline = row.find('a', class_='tab-link-news').getText().strip() # headline
            datetime_str = row.find('td', align='right').text.strip() # date of article
            standardized_datetime, current_date = parse_datetime(datetime_str, current_date)
            source = row.find('div', class_='news-link-right').span.text.strip('()') # source of article
            news_extracted.append([stock, standardized_datetime, headline, source])
            
    # convert to dataframe
    df = pd.DataFrame(news_extracted, columns=['Stock', 'Date', 'Headline', 'Source'])
    
    # Filter by date
    today = datetime.now()
    time_period = today - timedelta(days=period)
    df['Date'] = pd.to_datetime(df['Date'], format='%b-%d-%y %H:%M')
    df['Date'] = df['Date'].dt.tz_localize(None) # Remove timezone
    df = df[df['Date'] >= time_period]
    
    # Load BERT model
    model_name = "ahmedrachid/FinancialBERT-Sentiment-Analysis"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    sentiment_pipeline = pipeline("sentiment-analysis", model=model_name, tokenizer=tokenizer)
    model = AutoModelForSequenceClassification.from_pretrained(model_name)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)
    df['Sentiment'] = df['Headline'].apply(lambda x: sentiment_pipeline(x))
    
    df[['sentiment_label']] = df['Sentiment'].apply(lambda x: pd.Series(extract_label(x)))
    df[['sentiment_score']] = df['Sentiment'].apply(lambda x: pd.Series(extract_score(x)))
    return df

# def insert_stock_news(df: pd.DataFrame, cur: cursor):
def insert_stock_news(df: pd.DataFrame, conn_id: str):
    ''' Insert the stock news into the database'''

    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    cur = conn.cursor()

    tweet_query = """
        INSERT INTO Tweet_Headline (company_id, content, date) VALUES (%s, %s, %s) RETURNING tweet_id;
    """
    analysis_query = """
        INSERT INTO Analysis (tweet_id, sentiment_score, sentiment_label) VALUES (%s, %s, %s);
    """
    for index, row in df.iterrows():
        company_id = get_or_create_company_id(row['Stock'], cur)
        
        # Insert into Tweet_Headline table
        cur.execute(tweet_query, (company_id, row['Headline'], row['Date']))
        tweet_id = cur.fetchone()[0]  # Get the generated tweet_id
    
        # Insert into Analysis table
        cur.execute(analysis_query, (tweet_id, row['sentiment_score'], row['sentiment_label']))

    conn.commit()



# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Download stock news and insert into PostgreSQL database")
#     parser.add_argument("--stocks", nargs="+", help="List of stock tickers")
#     parser.add_argument("--period", help="Number of days of tweets extracted", default=1, type=int)
#     args = parser.parse_args()

#     if args.stocks:
#         stocks = args.stocks
#     else:
#         print("No stocks provided.")
#         print('Exiting...')
#         sys.exit()

#     news = download_stock_news(stocks)
#     df = process_news(news, args.period)
    
#     if df.empty:
#         print('No news downloaded')
#         print('Exiting...')
#         sys.exit()

#     params = {
#         'dbname': 'IS3107',
#         'user': 'postgres',
#         'password': 'is3107',
#         'host': 'localhost',
#         'port': 5432
#     }

#     conn = psycopg2.connect(**params)
#     cur = conn.cursor()

#     insert_stock_news(df, cur)

#     conn.commit()
#     conn.close()
    
#     print("Stock news inserted into database successfully.")