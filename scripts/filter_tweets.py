"""
Author: Eric Uehling
Date: 2024-07-12

Description: 

Outputs tweets about 'Mastodon' and 'Elon' from 2022 onward to separate csv files.
"""
import pandas as pd

def filter_and_save_tweets(input_file_path, output_mastadon_path, output_elon_path, userinfo_file_path):
    try:
        # Read in the tweets and userinfo data
        tweets_df = pd.read_csv(input_file_path, dtype={'RePEc_id': 'object', 'referenced_id': 'str', 'author_id': 'str', 'id': 'str'}, parse_dates=['created_at'], low_memory=False)
        userinfo_df = pd.read_csv(userinfo_file_path, dtype={'RePEc_id': 'object', 'id': 'str'})

        if 'text' not in tweets_df.columns:
            raise ValueError("The 'text' column was not found in the input file. Please check the file structure.")

        # Merge tweets_df with userinfo_df to populate RePEc_id based on author_id
        tweets_df = tweets_df.merge(userinfo_df[['id', 'RePEc_id']], left_on='author_id', right_on='id', how='left', suffixes=('', '_userinfo'))
        
        # Drop rows where RePEc_id is NaN after merging
        tweets_df = tweets_df.dropna(subset=['RePEc_id_userinfo'])

        # Replace the original RePEc_id with the one populated from userinfo, if available
        tweets_df['RePEc_id'] = tweets_df['RePEc_id_userinfo']
        tweets_df = tweets_df.drop(columns=['id_userinfo', 'RePEc_id_userinfo'])

        # Convert 'created_at' to timezone-naive UTC before filtering
        tweets_df['created_at'] = tweets_df['created_at'].dt.tz_localize(None)

        # Filter tweets by date, keeping only those after January 1, 2022
        tweets_df = tweets_df[tweets_df['created_at'] > pd.Timestamp('2022-01-01')]

        # Sort the entire dataset by 'created_at'
        tweets_df = tweets_df.sort_values(by='created_at')

        # Filter for case-insensitive search for 'mastodon' related keywords
        mastadon_keywords = ['mastodon', 'mastadon', 'mastadan']
        mastadon_tweets_df = tweets_df[tweets_df['text'].apply(lambda x: isinstance(x, str) and any(keyword.lower() in x.lower() for keyword in mastadon_keywords))]
        mastadon_tweets_df.to_csv(output_mastadon_path, index=False)

        # Filter for case-insensitive search for 'elon' and 'musk'
        elon_keywords = ['musk', 'elon']
        elon_tweets_df = tweets_df[tweets_df['text'].apply(lambda x: isinstance(x, str) and any(keyword.lower() in x.lower() for keyword in elon_keywords))]
        elon_tweets_df.to_csv(output_elon_path, index=False)

        print(f"Filtered data has been successfully saved to {output_mastadon_path} and {output_elon_path}")
    except FileNotFoundError:
        print(f"File {input_file_path} or {userinfo_file_path} not found. Please check the file paths.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Define file paths
input_file_path = '../data/csv/cleaned_RePEc_tweets.csv'
userinfo_file_path = '../data/csv/cleaned_RePEc_userinfo.csv'
output_mastadon_path = '../data/csv/mastadon_tweets.csv'
output_elon_path = '../data/csv/elon_tweets.csv'

if __name__ == "__main__":
    filter_and_save_tweets(input_file_path, output_mastadon_path, output_elon_path, userinfo_file_path)
