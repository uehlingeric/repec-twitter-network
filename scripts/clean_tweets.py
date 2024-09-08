"""
Author: Eric Uehling
Date: 2024-02-22

Description: 

Reads in RePEc_tweets.csv and cleans the data row by row, fixing the structure of the csv file that was misformatted due to the 'text' field. 
Returns the cleaned file as cleaned_RePEc_tweets.csv.
"""
import pandas as pd
import csv
from io import StringIO


def is_row_complete(row, num_columns):
    try:
        parsed_row = next(csv.reader([row]))  # Handles commas within quotes
        return len(parsed_row) == num_columns
    except Exception as e:
        print(f"Error parsing row: {e}")
        return False


def clean_and_process_csv(file_path, final_cleaned_file_path):
    cleaned_data = StringIO()
    writer = csv.writer(cleaned_data)

    with open(file_path, 'r', encoding='utf-8') as infile:
        reader = infile.readlines()

        num_columns = len(reader[0].strip().split(','))
        writer.writerow(reader[0].strip().split(','))  # Writes the header

        accumulated_line = ''
        for line in reader[1:]:
            concatenated_line = ' '.join(
                [accumulated_line, line.strip()]).strip()
            if is_row_complete(concatenated_line, num_columns):
                writer.writerow(next(csv.reader([concatenated_line])))
                accumulated_line = ''
            else:
                accumulated_line = concatenated_line

    cleaned_data.seek(0)  # Reset StringIO cursor to the beginning

    # Loads the cleaned CSV data from StringIO into the DataFrame
    data = pd.read_csv(cleaned_data,
                       dtype={'referenced_id': str,
                              'id': str,
                              'author_id': str,
                              'RePEc_id': str,
                              'created_at': str,
                              'text': str,
                              'referenced_type': str,
                              'referenced_id': str,
                              'lang': str})

    # Ensures that the numeric columns are of the correct type
    numeric_columns = ['retweet_count', 'reply_count',
                       'like_count', 'quote_count', 'impression_count']
    for col in numeric_columns:
        data[col] = pd.to_numeric(
            data[col], errors='coerce').astype(pd.Int64Dtype())

    data['referenced_id'] = data['referenced_id'].fillna('NA')
    data['referenced_type'] = data['referenced_type'].fillna('own')
    data['created_at'] = pd.to_datetime(data['created_at'], errors='coerce')
    data['created_at'] = data['created_at'].dt.tz_localize(
        None).dt.tz_localize('UTC')
    data = data.dropna(subset=numeric_columns + ['created_at'])

    # Updates impression_count to NA for tweets with created_at year > 2022 and impression_count = 0
    condition = (data['created_at'].dt.year < 2023) & (
        data['impression_count'] == 0)
    data.loc[condition, 'impression_count'] = pd.NA

    data.to_csv(final_cleaned_file_path, index=False)


if __name__ == "__main__":
    file_path = '../data/csv/RePEc_tweets.csv'
    final_cleaned_file_path = '../data/csv/cleaned_RePEc_tweets.csv'
    clean_and_process_csv(file_path, final_cleaned_file_path)
    print("CSV file has been cleaned and processed.")
