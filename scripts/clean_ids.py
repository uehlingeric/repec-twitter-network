"""
Author: Eric Uehling
Date: 2024-04-08

Description: 

Ultimately cleans the tweets 'RePEc_id' columns through a 3 frame merge.
"""
import pandas as pd


def preprocess_twitter_accounts(economists_info_path, repec_userinfo_path, output_userinfo_path):
    """
    Preprocess and merge economists' info with RePEc userinfo based on Twitter accounts.
    """
    try:
        # Load the CSV files
        economists_info = pd.read_csv(economists_info_path)
        repec_userinfo = pd.read_csv(repec_userinfo_path, dtype={'id': 'str'})
      
        # Preprocess the Twitter Account to match the username format
        economists_info['Modified Twitter Account'] = economists_info['Twitter Account'].str.replace('@', '').str.replace(' ', '', regex=False).str.lower()

        # Also ensure usernames in repec_userinfo are in lowercase for case-insensitive comparison
        repec_userinfo['username'] = repec_userinfo['username'].str.lower()

        # Merge the two dataframes based on the modified Twitter Account and username
        # 'left' ensures all entries in repec_userinfo are retained, even if no match is found
        merged_df = repec_userinfo.merge(economists_info, left_on='username', right_on='Modified Twitter Account', how='left')

        # Drop temporary columns and rename 'RePEc Short-ID'
        final_df = merged_df.drop(columns=['Modified Twitter Account', 'Name', 'Twitter Account']).rename(
            columns={'RePEc Short-ID': 'RePEc_id'})

        # Save the updated dataframe
        final_df.to_csv(output_userinfo_path, index=False)
        print(
            f"Merged userinfo has been successfully saved to {output_userinfo_path}")
    except FileNotFoundError:
        print("One of the input files was not found. Please check the file paths.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def update_tweets_with_repec_id(cleaned_tweets_path, updated_userinfo_path, output_tweets_path):
    """
    Fill missing 'RePEc_id' values in the tweets dataset using 'RePEc_id' from the updated userinfo dataset.
    """
    try:
        # Load the datasets
        updated_userinfo = pd.read_csv(updated_userinfo_path, dtype={
                                       'id': 'str', 'RePEc_id': 'str'})
        cleaned_tweets = pd.read_csv(cleaned_tweets_path, dtype={
                                     'author_id': 'str', 'RePEc_id': 'str'}, low_memory=False)

        # Find non-unique 'id' values in the updated userinfo dataset
        duplicate_ids = updated_userinfo[updated_userinfo.duplicated('id', keep=False)]['id'].unique()
        if len(duplicate_ids) > 0:
            print(f"The 'id' column in the updated userinfo dataset is not unique. Non-unique ids: {duplicate_ids}")
            return  # Stop execution if there are non-unique IDs

        # Identify rows with missing 'RePEc_id' and fill them
        missing_repec_ids = cleaned_tweets['RePEc_id'].isnull()
        id_map = updated_userinfo.drop_duplicates(
            'id').set_index('id')['RePEc_id']
        cleaned_tweets.loc[missing_repec_ids, 'RePEc_id'] = cleaned_tweets.loc[missing_repec_ids, 'author_id']\
            .map(id_map)

        # Save the updated tweets dataset
        cleaned_tweets.to_csv(output_tweets_path, index=False)
        print(
            f"Updated tweets have been successfully saved to {output_tweets_path}")
    except FileNotFoundError:
        print("One of the input files was not found. Please check the file paths.")
    except ValueError as ve:
        print(ve)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    economists_info_path = '../data/csv/Economists_Info.csv'
    repec_userinfo_path = '../data/csv/RePEc_userinfo.csv'
    cleaned_tweets_path = '../data/csv/cleaned_RePEc_tweets.csv'
    output_userinfo_path = '../data/csv/cleaned_RePEc_userinfo.csv'
    output_tweets_path = '../data/csv/cleaned_RePEc_tweets.csv'

    preprocess_twitter_accounts(
        economists_info_path, repec_userinfo_path, output_userinfo_path)
    update_tweets_with_repec_id(
        cleaned_tweets_path, output_userinfo_path, output_tweets_path)
