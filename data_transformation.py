import pandas as pd
import numpy as np
from datetime import datetime
from abc import ABC, abstractmethod
from data_ingestion import restaurants_df, reviews_df

class DataCleaner(ABC):
    def __init__(self, df):
        self.df = df

    @abstractmethod
    def clean(self):
        pass

    def remove_duplicates(self, subset, keep='last'):
        self.df.drop_duplicates(subset=subset, keep=keep, inplace=True)

    def standardize_text(self, column):
        self.df[column] = self.df[column].str.title()

class RestaurantDataCleaner(DataCleaner):
    def clean(self):
        self.remove_duplicates(subset=['id'])
        self.standardize_text('name')
        self.standardize_text('city')
        self.clean_rating()
        self.clean_cost()
        self.clean_cuisine()
        self.clean_address()
        return self.df

    def clean_rating(self):
        self.df['rating'] = pd.to_numeric(self.df['rating'].replace('--', 'NaN'), errors='coerce')
        self.df['rating_count'] = self.df['rating_count'].replace('Too Few Ratings', '0')
        self.df['rating_count'] = self.df['rating_count'].str.extract('(\d+)').astype(float)

    def clean_cost(self):
        self.df['cost'] = self.df['cost'].str.replace('₹', '', regex=False)
        self.df['cost'] = pd.to_numeric(self.df['cost'].replace('NA', np.nan), errors='coerce')

    def clean_cuisine(self):
        self.df['cuisine'] = self.df['cuisine'].str.split(',')

    def clean_address(self):
        self.df['address'] = self.df['address'].str.strip()

class ReviewDataCleaner(DataCleaner):
    def clean(self):
        self.remove_duplicates(subset=['Id'])
        self.standardize_text('ProfileName')
        self.clean_helpfulness()
        self.clean_time()
        self.clean_text_fields()
        return self.df

    def clean_helpfulness(self):
        self.df['HelpfulnessNumerator'] = pd.to_numeric(self.df['HelpfulnessNumerator'], errors='coerce')
        self.df['HelpfulnessDenominator'] = pd.to_numeric(self.df['HelpfulnessDenominator'], errors='coerce')
        self.df['HelpfulnessRatio'] = self.df['HelpfulnessNumerator'] / self.df['HelpfulnessDenominator']
        self.df['HelpfulnessRatio'] = self.df['HelpfulnessRatio'].fillna(0)

    def clean_time(self):
        self.df['Time'] = pd.to_datetime(self.df['Time'], unit='s')

    def clean_text_fields(self):
        for column in ['Summary', 'Text']:
            self.df[column] = self.df[column].str.strip()
            self.df[column] = self.df[column].str.replace('\n', ' ')

# Keeping below part for the testing purpose locally
# Example usage
# if __name__ == "__main__":
#     # Ensure data_ingestion.py has been run
#     restaurants_cleaner = RestaurantDataCleaner(restaurants_df)
#     cleaned_restaurants = restaurants_cleaner.clean()

#     reviews_cleaner = ReviewDataCleaner(reviews_df)
#     cleaned_reviews = reviews_cleaner.clean()

#     # Set display options to show all columns
#     pd.set_option('display.max_columns', None)

#     # Print the first few rows of each DataFrame to verify the data
#     print("Restaurants DataFrame:")
#     print(cleaned_restaurants.head())
#     print("\nReviews DataFrame:")
#     print(cleaned_reviews.head())
