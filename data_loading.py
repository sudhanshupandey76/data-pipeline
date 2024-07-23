import os
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extras import execute_values
from abc import ABC, abstractmethod
from data_transformation import cleaned_restaurants, cleaned_reviews

class DatabaseOperator(ABC):
    def __init__(self, connection_params):
        self.connection_params = connection_params

    @abstractmethod
    def execute(self, df, table_name):
        pass

class RedshiftLoader(DatabaseOperator):
    def execute(self, df, table_name):
        connection_string = f"postgresql://{self.connection_params['user']}:{self.connection_params['password']}@{self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['database']}"
        engine = create_engine(connection_string)
        df.to_sql(table_name, engine, if_exists='append', index=False)

class RedshiftUpserter(DatabaseOperator):
    def __init__(self, connection_params, unique_key):
        super().__init__(connection_params)
        self.unique_key = unique_key

    def execute(self, df, table_name):
        conn = psycopg2.connect(
            host=self.connection_params['host'],
            port=self.connection_params['port'],
            dbname=self.connection_params['database'],
            user=self.connection_params['user'],
            password=self.connection_params['password']
        )
        cur = conn.cursor()
        
        try:
            # Create a temp table
            cur.execute(f"CREATE TEMP TABLE temp_{table_name} (LIKE {table_name})")
            
            # Insert data into temp table
            columns = df.columns.tolist()
            values = [tuple(x) for x in df.to_numpy()]
            query = f"INSERT INTO temp_{table_name} ({','.join(columns)}) VALUES %s"
            execute_values(cur, query, values)
            
            # Perform upsert
            cur.execute(f"""
                UPDATE {table_name} t
                SET {', '.join([f"{col} = temp.{col}" for col in columns if col != self.unique_key])}
                FROM temp_{table_name} temp
                WHERE t.{self.unique_key} = temp.{self.unique_key};
                
                INSERT INTO {table_name}
                SELECT temp.*
                FROM temp_{table_name} temp
                LEFT JOIN {table_name} t ON t.{self.unique_key} = temp.{self.unique_key}
                WHERE t.{self.unique_key} IS NULL;
            """)
            
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()
            conn.close()

class DatabaseManager:
    def __init__(self):
        self.connection_params = {
            'host': os.environ.get('REDSHIFT_HOST'),
            'port': os.environ.get('REDSHIFT_PORT', '5439'),
            'database': os.environ.get('REDSHIFT_DATABASE'),
            'user': os.environ.get('REDSHIFT_USER'),
            'password': os.environ.get('REDSHIFT_PASSWORD')
        }

    def load_data(self, df, table_name, operation_type, unique_key=None):
        if operation_type == 'load':
            operator = RedshiftLoader(self.connection_params)
        elif operation_type == 'upsert':
            if unique_key is None:
                raise ValueError("Unique key is required for upsert operation")
            operator = RedshiftUpserter(self.connection_params, unique_key)
        else:
            raise ValueError("Invalid operation type. Use 'load' or 'upsert'")

        operator.execute(df, table_name)

# Usage
db_manager = DatabaseManager()

# For restaurants data
db_manager.load_data(cleaned_restaurants, 'restaurants', 'upsert', unique_key='id')

# For reviews data
db_manager.load_data(cleaned_reviews, 'reviews', 'upsert', unique_key='Id')