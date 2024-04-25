import gspread
import pandas as pd
import torch
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline

from stock_price import get_or_create_company_id

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
    time_period = today - timedelta(hours=period)
    all_data_df['Date'] = pd.to_datetime(all_data_df['Mock_Date'])
    all_data_df['Date'] = all_data_df['Date'].dt.tz_localize(None) # Remove timezone
    all_data_df = all_data_df[(all_data_df['Date'] >= time_period) & (all_data_df['Date'] <= today)]
    
    # Filter by stocks
    all_data_df = all_data_df[all_data_df['Stock Name'].isin(stocks)]
    
    print(f"Num Tweets Extracted: {len(all_data_df)}")

    return all_data_df


def process_tweets(df: pd.DataFrame, conn_id: str) -> pd.DataFrame:
    ''' Perform sentiment analysis on tweets using BERT model, and return the processed dataframe. '''
    if df.empty:
        print('No tweets to process')
        return df
    
    # For each stock, add the respective company_id from Company table (foreign key)
    df['company_id'] = df['Stock Name'].apply(lambda x: get_or_create_company_id(x, conn_id=conn_id))

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
        # Insert into Tweet_Headline table
        cur.execute(tweet_query, (row['company_id'], row['Tweet'], row['Date']))
        tweet_id = cur.fetchone()[0]  # Get the generated tweet_id
    
        # Insert into Analysis table
        cur.execute(analysis_query, (tweet_id, row['sentiment_score'], row['sentiment_label']))

    conn.commit()
    cur.close()
    conn.close()
    print(f'Inserted {len(df)} tweets into the database.')