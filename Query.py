import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook

class Query:
    def __init__(self, conn_id: str) -> None:
        self.conn_id = conn_id
        self.hook = PostgresHook(postgres_conn_id=conn_id)
        self.conn = self.hook.get_conn()
        self.cur = self.conn.cursor()

    def exit(self) -> bool:
        self.cur.close()
        self.conn.close()

    def avg_stock_sentiment(self, stock: str) -> pd.DataFrame:
        query = """
            SELECT
                CAST(p.date AS DATE) AS Date,
                AVG(a.sentiment_score) AS Average_Sentiment_Score,
                c.company_name AS Company_Name,
                c.ticker_symbol AS Stock_Symbol
            FROM
                Analysis a
            JOIN
                Tweet_Headline th ON a.tweet_id = th.tweet_id
            JOIN
                Price p ON th.company_id = p.company_id AND CAST(th.date AS DATE) = CAST(p.date AS DATE)
            JOIN
                Company c ON p.company_id = c.company_id
            WHERE c.ticker_symbol = %s
            GROUP BY
                CAST(p.date AS DATE), c.company_name, c.ticker_symbol
            ORDER BY
                CAST(p.date AS DATE);
            """
        self.cur.execute(query, (stock,))
        data = self.cur.fetchall()
        columns = [desc[0] for desc in self.cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    def avg_stock_price(self, stock: str) -> pd.DataFrame:
        query = """
            SELECT
                CAST(p.date AS DATE) AS Date,
                AVG(p.close_price) AS Average_Stock_Price,
                c.company_name AS Company_Name,
                c.ticker_symbol AS Stock_Symbol
            FROM
                Price p
            JOIN
                Company c ON p.company_id = c.company_id
            WHERE c.ticker_symbol = %s
            GROUP BY
                CAST(p.date AS DATE), c.company_name, c.ticker_symbol
            ORDER BY
                CAST(p.date AS DATE);
            """
        self.cur.execute(query, (stock,))
        data = self.cur.fetchall()
        columns = [desc[0] for desc in self.cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    def top_mentioned_stocks(self) -> pd.DataFrame:
        query = """
            SELECT
                c.ticker_symbol AS Stock_Symbol,
                c.company_name AS Company_Name,
                COUNT(*) AS Count_of_Mentions
            FROM
                Tweet_Headline th
            JOIN
                Company c ON th.company_id = c.company_id
            WHERE
                CAST(th.date AS DATE) >= current_date - interval '7 days'
            GROUP BY
                c.ticker_symbol,
                c.company_name
            ORDER BY
                Count_of_Mentions DESC;
            """
        self.cur.execute(query)
        data = self.cur.fetchall()
        columns = [desc[0] for desc in self.cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    def most_active_trading_days(self) -> pd.DataFrame:
        query = """
            SELECT
                CAST(p.date AS DATE) AS Date,
                SUM(p.volume) AS Trading_Volume,
                COUNT(th.tweet_id) AS Number_of_Tweets_News
            FROM
                Price p
            LEFT JOIN
                Tweet_Headline th ON CAST(p.date AS DATE) = CAST(th.date AS DATE)
            GROUP BY
                CAST(p.date AS DATE)
            ORDER BY
                SUM(p.volume) DESC;
            """
        self.cur.execute(query)
        data = self.cur.fetchall()
        columns = [desc[0] for desc in self.cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    def most_active_trading_days_stock(self) -> pd.DataFrame:
        query = """
            SELECT
                CAST(p.date AS DATE) AS Date,
                c.ticker_symbol AS Stock,
                c.company_name AS Company_Name,
                SUM(p.volume) AS Trading_Volume,
                COUNT(th.tweet_id) AS Number_of_Tweets_News
            FROM
                Price p
            JOIN
                Company c ON p.company_id = c.company_id
            LEFT JOIN
                Tweet_Headline th ON CAST(p.date AS DATE) = CAST(th.date AS DATE) AND p.company_id = th.company_id
            GROUP BY
                CAST(p.date AS DATE),
                c.ticker_symbol,
                c.company_name
            ORDER BY
                SUM(p.volume) DESC;
            """
        self.cur.execute(query)
        data = self.cur.fetchall()
        columns = [desc[0] for desc in self.cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    def tweet_headline_sentiment(self) -> pd.DataFrame:
        query = """
            SELECT
                CAST(th.Date AS DATE) AS Date,
                th.content AS Tweets_News_Headline,
                a.sentiment_label AS Sentiment_Label,
                a.sentiment_score AS Sentiment_Score
            FROM
                Tweet_Headline th
            JOIN
                Analysis a ON th.tweet_id = a.tweet_id;
            """
        self.cur.execute(query)
        data = self.cur.fetchall()
        columns = [desc[0] for desc in self.cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    def sector_sentiment(self) -> pd.DataFrame:
        query = """
            SELECT
                CAST(p.date AS DATE) AS Date,
                c.sector AS Sector,
                AVG(a.sentiment_score) AS Average_Sentiment_Score,
                AVG(p.close_price) AS Average_Price
            FROM
                Price p
            JOIN
                Company c ON p.company_id = c.company_id
            LEFT JOIN
                Tweet_Headline th ON CAST(p.date AS DATE) = CAST(th.date AS DATE) AND p.company_id = th.company_id
            LEFT JOIN
                Analysis a ON th.tweet_id = a.tweet_id
            GROUP BY
                CAST(p.date AS DATE),
                c.sector
            ORDER BY
                CAST(p.date AS DATE);
            """
        self.cur.execute(query)
        data = self.cur.fetchall()
        columns = [desc[0] for desc in self.cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    def avg_daily_sentiment(self, stock: str) -> pd.DataFrame:
        query = """
            SELECT
                CAST(th.date AS DATE) AS Date,
                c.company_id,
                c.company_name,
                c.ticker_symbol,
                CASE
                    WHEN a.sentiment_label = 'positive' THEN 1
                    WHEN a.sentiment_label = 'neutral' THEN 0
                    WHEN a.sentiment_label = 'negative' THEN -1
                END AS Sentiment_Value
            FROM
                Company c
            JOIN
                Tweet_Headline th ON c.company_id = th.company_id
            LEFT JOIN
                Analysis a ON th.tweet_id = a.tweet_id
            WHERE c.ticker_symbol = %s
            """
        self.cur.execute(query, (stock,))
        data = self.cur.fetchall()
        columns = [desc[0] for desc in self.cur.description]
        df = pd.DataFrame(data, columns=columns)
        
        # Check the column names in the DataFrame
        print(df.columns)
        
        # Aggregate sentiment values on a daily basis for each company
        aggregated_df = df.groupby(['date', 'company_id', 'company_name'])['sentiment_value'].mean().reset_index()
        return aggregated_df