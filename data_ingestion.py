import json
import sqlite3
import pandas as pd
from abc import ABC, abstractmethod

class DataLoader(ABC):
    @abstractmethod
    def load_data(self):
        pass

class JSONDataLoader(DataLoader):
    def __init__(self, file_path):
        self.file_path = file_path

    def load_data(self):
        try:
            with open(self.file_path, 'r') as file:
                data = json.load(file)

            restaurants_list = []

            for city, city_data in data.items():
                city_link = city_data.get('link')

                if 'restaurants' in city_data:
                    for rest_id, rest_data in city_data['restaurants'].items():
                        restaurant_info = {
                            'id': rest_id,
                            'name': rest_data.get('name'),
                            'city': city,
                            'rating': rest_data.get('rating'),
                            'rating_count': rest_data.get('rating_count'),
                            'cost': rest_data.get('cost'),
                            'cuisine': rest_data.get('cuisine'),
                            'lic_no': rest_data.get('lic_no'),
                            'link': city_link,
                            'address': rest_data.get('address')
                        }
                        restaurants_list.append(restaurant_info)

            return pd.DataFrame(restaurants_list)

        except FileNotFoundError:
            print(f"Error: The file at {self.file_path} was not found.")
            return pd.DataFrame()  # Return an empty DataFrame
        except json.JSONDecodeError:
            print("Error: Failed to decode JSON from the file.")
            return pd.DataFrame()  # Return an empty DataFrame
        except Exception as e:
            print(f"An unexpected error occurred while loading JSON data: {e}")
            return pd.DataFrame()  # Return an empty DataFrame

class SQLiteDataLoader(DataLoader):
    def __init__(self, db_path, query):
        self.db_path = db_path
        self.query = query

    def load_data(self):
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(self.query, conn)
            conn.close()
            return df
        except sqlite3.OperationalError as e:
            print(f"Error: Database operation failed: {e}")
            return pd.DataFrame()  # Return an empty DataFrame
        except sqlite3.DatabaseError as e:
            print(f"Error: Database error occurred: {e}")
            return pd.DataFrame()  # Return an empty DataFrame
        except Exception as e:
            print(f"An unexpected error occurred while loading SQLite data: {e}")
            return pd.DataFrame()  # Return an empty DataFrame

class DataLoaderManager:
    def __init__(self, json_loader, sqlite_loader):
        self.json_loader = json_loader
        self.sqlite_loader = sqlite_loader

    def load_all_data(self):
        restaurants_df = self.json_loader.load_data()
        reviews_df = self.sqlite_loader.load_data()
        return restaurants_df, reviews_df

# Usage
json_loader = JSONDataLoader('/Users/sudhanshu/data-pipeline/data.json')
sqlite_loader = SQLiteDataLoader('/Users/sudhanshu/data-pipeline/database.sqlite', 'SELECT * FROM reviews')
data_manager = DataLoaderManager(json_loader, sqlite_loader)

restaurants_df, reviews_df = data_manager.load_all_data()

# Set display options to show all columns
pd.set_option('display.max_columns', None)

# Print the first few rows of each DataFrame to verify the data
print("Restaurants DataFrame:")
print(restaurants_df.head())
print("\nReviews DataFrame:")
print(reviews_df.head())
