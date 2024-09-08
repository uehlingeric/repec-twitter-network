"""
Author: Eric Uehling
Date: 2024-07-14

Description: 

Test script for interaction algorithm.
"""
import pandas as pd

def initialize_mentions_interacted_df(userinfo_df, max_week):
    """
    Initialize the mentions or interacted DataFrame with week columns set to 0.
    """
    week_columns = [f'week{week_num}' for week_num in range(1, max_week + 1)]
    df = userinfo_df[['id', 'RePEc_id', 'followers_count', 'following_count']].copy()
    df.columns = ['author_id', 'repec_id', 'number_of_followers', 'following_count']
    for week_col in week_columns:
        df[week_col] = 0
    return df

def create_following_dict(repec_following_df):
    """
    Create a dictionary mapping repec_id to a list of their follower_repec_ids.
    """
    return repec_following_df.groupby('repec_id')['follower_repec_id'].apply(list).to_dict()

def update_mentions_df(mentions_df, mastodon_user_week_df):
    """
    Update the mentions DataFrame based on mastodon_user_week data.
    """
    for _, row in mastodon_user_week_df.iterrows():
        week_num = row['week']
        repec_users = row['repec_users'][1:-1].replace("'", "").split(", ")
        print(f"Processing mentions for week {week_num}: {repec_users}")

        for repec_id in repec_users:
            if repec_id in mentions_df['repec_id'].values:
                mentions_df.loc[mentions_df['repec_id'] == repec_id, f'week{week_num}'] = 1
                print(f"Marked {repec_id} as mentioned in week {week_num}")
    return mentions_df

def update_interacted_df(interacted_df, mastodon_user_week_df, following_dict):
    """
    Update the interacted DataFrame based on mastodon_user_week and repec_following data.
    """
    for _, row in mastodon_user_week_df.iterrows():
        week_num = row['week']
        repec_users = row['repec_users'][1:-1].replace("'", "").split(", ")
        print(f"Processing interactions for week {week_num}: {repec_users}")

        for repec_id in repec_users:
            if repec_id in interacted_df['repec_id'].values:
                interacted_df.loc[interacted_df['repec_id'] == repec_id, f'week{week_num}'] = 1
                print(f"Marked {repec_id} as interacted in week {week_num}")

            if repec_id in following_dict:
                followers = following_dict[repec_id]
                for follower_id in followers:
                    if follower_id in repec_users:
                        interacted_df.loc[interacted_df['repec_id'] == repec_id, f'week{week_num}'] = 1
                        print(f"Marked {repec_id} as interacted in week {week_num} due to follower {follower_id}")
    return interacted_df

def run_mentions_interacted_algorithm(userinfo_df, repec_following_df, mastodon_user_week_df):
    """
    Execute the entire mentions and interacted algorithm workflow.
    """
    max_week = mastodon_user_week_df['week'].max()
    mentions_df = initialize_mentions_interacted_df(userinfo_df, max_week)
    interacted_df = initialize_mentions_interacted_df(userinfo_df, max_week)
    following_dict = create_following_dict(repec_following_df)

    print("\n--- Running Mentions Algorithm ---")
    mentions_df = update_mentions_df(mentions_df, mastodon_user_week_df)
    
    print("\n--- Running Interacted Algorithm ---")
    interacted_df = update_interacted_df(interacted_df, mastodon_user_week_df, following_dict)
    
    return mentions_df, interacted_df

# Sample data for testing
def generate_sample_data():
    userinfo_df = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'RePEc_id': ['repec_user1', 'repec_user2', 'repec_user3', 'repec_user4'],
        'followers_count': [100, 150, 200, 250],
        'following_count': [50, 60, 70, 80]
    })

    repec_following_df = pd.DataFrame({
        'repec_id': ['repec_user1', 'repec_user1', 'repec_user2'],
        'follower_repec_id': ['repec_user2', 'repec_user3', 'repec_user4']
    })

    mastodon_user_week_df = pd.DataFrame({
        'week': [1, 2, 3],
        'repec_users': ["['repec_user1', 'repec_user3']", "['repec_user2']", "['repec_user4']"]
    })

    return userinfo_df, repec_following_df, mastodon_user_week_df

def test_mentions_interacted():
    print("Generating sample data...")
    userinfo_df, repec_following_df, mastodon_user_week_df = generate_sample_data()

    print("\nSample User Info DataFrame:")
    print(userinfo_df)
    print("\nSample RePEc Following DataFrame:")
    print(repec_following_df)
    print("\nSample Mastodon User Week DataFrame:")
    print(mastodon_user_week_df)

    print("\nRunning the mentions and interacted algorithms...")
    mentions_df, interacted_df = run_mentions_interacted_algorithm(userinfo_df, repec_following_df, mastodon_user_week_df)

    print("\n--- Final Mentions DataFrame ---")
    print(mentions_df)

    print("\n--- Final Interacted DataFrame ---")
    print(interacted_df)

if __name__ == "__main__":
    test_mentions_interacted()
