import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import connection, cursor

class Connection:
    def __init__(self) -> None:
        # self.dbname = None
        # self.user = None
        # self.password = None
        # self.host = None
        # self.port = None
        self.conn = None
        self.cur = None

    # def connect(self, dbname: str, user: str, password: str, host: str, port: int) -> None:
    def connect(self, conn_id: str) -> None:
        # self.dbname = dbname
        # self.user = user
        # self.password = password
        # self.host = host
        # self.port = port
        # params = {
        #     'dbname': self.dbname,
        #     'user': self.user,
        #     'password': self.password,
        #     'host': self.host,
        #     'port': self.port
        # }
        # self.conn: connection = psycopg2.connect(**params)
        # self.cur: cursor = self.conn.cursor()

        hook = PostgresHook(postgres_conn_id=conn_id)
        self.conn = hook.get_conn()
        self.cur = self.conn.cursor()

    def exit(self) -> bool:
        self.conn.commit()
        self.cur.close()
        self.conn.close()
    
    def init_db(self) -> cursor:
        if not self.conn:
            self.connect()
        try: 
            query = '''
                CREATE table if not EXISTS Company ( 
                    company_id SERIAL PRIMARY KEY, 
                    company_name VARCHAR(255) NOT NULL, 
                    ticker_symbol VARCHAR(10) UNIQUE NOT NULL, 
                    sector VARCHAR(100) 
                ); 
                
                CREATE TABLE if not exists Price ( 
                    price_id SERIAL PRIMARY KEY, 
                    company_id INT REFERENCES Company(company_id), 
                    date DATE NOT NULL, 
                    open_price DECIMAL(12, 2), 
                    close_price DECIMAL(12, 2), 
                    high_price DECIMAL(12, 2), 
                    low_price DECIMAL(12, 2), 
                    dividends DECIMAL(12, 2), 
                    volume INT, 
                    stock_splits INT 
                ); 
                
                CREATE TABLE if not exists Tweet_Headline ( 
                    tweet_id SERIAL PRIMARY KEY, 
                    company_id INT REFERENCES Company(company_id), 
                    content TEXT, 
                    date DATE NOT NULL 
                ); 
                
                CREATE TABLE if not exists Analysis (
                    analysis_id SERIAL PRIMARY KEY,
                    tweet_id INT REFERENCES Tweet_Headline(tweet_id),
                    sentiment_score FLOAT,
                    sentiment_label TEXT
                );
            '''
            self.cur.execute(query)
            self.conn.commit()
            # return self.cur
        except Exception as e:
            print(f'Error initializing database: {e}')