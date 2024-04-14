import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from torch.nn.functional import softmax
import torch
import yfinance as yf
import psycopg2
from datetime import datetime, timedelta
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

def extract_label(row):
    return row[0]['label']

def extract_score(row):
    return row[0]['score']

def extract_tweets(stocks: list, period: int=1) -> pd.DataFrame:
    ''' 
    Extract stock tweets from Google Drive which contains our twitter data. 
    Period specifies number of days of tweets extracted.
    '''
    # define scope and obtain credentials
    scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',
            "https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

    creds = ServiceAccountCredentials.from_json_keyfile_name('token.json', scope)

    # authentication
    client = gspread.authorize(creds)
    
    # Extract all stock tweet csvs
    service = build('drive', 'v3', credentials=creds)
    query = "mimeType='application/vnd.google-apps.spreadsheet' and name = 'stock_tweets'"

    results = service.files().list(q=query,
                                spaces='drive',
                                fields='nextPageToken, files(id, name)').execute()

    items = results.get('files', [])
    all_data_df = pd.DataFrame()

    if not items:
        print('No files found.')
    else:
        for item in items:
            sh = client.open_by_key(item['id'])
            worksheet = sh.get_worksheet(0) # First worksheet
            data = worksheet.get_all_values() # Get values
        
            # Store in df
            df = pd.DataFrame(data)
            df.columns = df.iloc[0]
            df = df.iloc[1:].reset_index(drop=True)
            all_data_df = pd.concat([all_data_df, df], ignore_index=True)
    
    
    # Filter by date
    today = datetime.now()
    time_period = today - timedelta(days=period)
    all_data_df['Date'] = pd.to_datetime(all_data_df['Date'])
    all_data_df['Date'] = all_data_df['Date'].dt.tz_localize(None) # Remove timezone
    all_data_df = all_data_df[all_data_df['Date'] >= time_period]
    
    # Filter by stocks
    all_data_df = all_data_df[all_data_df['Stock Name'].isin(stocks)]
    
    print(f"Num Tweets Extracted: {len(all_data_df)}")

    return all_data_df


def process_tweets(df: pd.DataFrame) -> pd.DataFrame:
    ''' Perform sentiment analysis on tweets using BERT model, and return the processed dataframe. '''
        # Import BERT model
    model_name = "ahmedrachid/FinancialBERT-Sentiment-Analysis"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    sentiment_pipeline = pipeline("sentiment-analysis", model=model_name, tokenizer=tokenizer)
    model = AutoModelForSequenceClassification.from_pretrained(model_name)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)
    df['Sentiment'] = df['Tweet'].apply(lambda x: sentiment_pipeline(x))
    
    df[['sentiment_label']] = df['Sentiment'].apply(lambda x: pd.Series(extract_label(x)))
    df[['sentiment_score']] = df['Sentiment'].apply(lambda x: pd.Series(extract_score(x)))
    
    return df
    
# def insert_stock_tweets(df: pd.DataFrame, cur: cursor) -> None:
def insert_stock_tweets(df: pd.DataFrame, conn_id: str) -> None:
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
        company_id = get_or_create_company_id(row['Stock Name'], cur)
        
        # Insert into Tweet_Headline table
        cur.execute(tweet_query, (company_id, row['Tweet'], row['Date']))
        tweet_id = cur.fetchone()[0]  # Get the generated tweet_id
    
        # Insert into Analysis table
        cur.execute(analysis_query, (tweet_id, row['sentiment_score'], row['sentiment_label']))

    conn.commit()


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Download stock tweets and insert into PostgreSQL database")
#     parser.add_argument("--stocks", nargs="+", help="List of stock tickers")
#     parser.add_argument("--period", help="Number of days of tweets extracted", default=1, type=int)
#     args = parser.parse_args()

#     if args.stocks:
#         stocks = args.stocks
#     else:
#         print("No stocks provided.")
#         print('Exiting...')
#         sys.exit()

#     tweets = extract_tweets(stocks, period=args.period)
    
#     if len(tweets) == 0:
#         print('No tweets extracted')
#         print('Exiting...')
#         sys.exit()
    
#     df = process_tweets(tweets)
        
#     params = {
#         'dbname': 'IS3107',
#         'user': 'postgres',
#         'password': 'is3107',
#         'host': 'localhost',
#         'port': 5432
#     }

#     conn = psycopg2.connect(**params)
#     cur = conn.cursor()

#     insert_stock_tweets(df, cur)

#     conn.commit()
#     conn.close()
    
#     print("Stock news inserted into database successfully.")