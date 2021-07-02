import pandas as pd
from mongoengine import QuerySet
from json import loads

from lib.db.queries.tweet_queries import get_tweets_for_search_query
from lib.db.connection import connect_to_mongo
from lib.db.queries.tweet_mutations import add_attribute_to_tweet
from lib.db.helpers import query_set_to_df
from lib.preprocessing.tweet_type import tweet_type
from lib.preprocessing.misc_attribute_helpers import contains_url
from lib.preprocessing.user_type import user_type

from typing import List, Dict, Tuple


def add_attributes_to_tweets(tweets: QuerySet, attributes: List[str]):
    if 'user_type' in attributes:
        user_groups = calculate_user_groups(tweets)

    for i, tweet in enumerate(tweets):
        tweet_dict = loads(tweet.to_json())
        if(i % 1000 == 0):
            print(f'Added attributes for {i} tweets. Continuing...')
        for attribute in attributes:
            if attribute == 'tweet_type':
                value = tweet_type(tweet_dict)
            elif attribute == 'contains_url':
                value =  contains_url(tweet_dict)
            elif attribute == 'user_type':
                value = user_type(tweet_dict, user_groups)
            else:
                raise ValueError(f'No know preprocessing operation exists for adding the attribute {attribute}')
            add_attribute_to_tweet(tweet, attribute, value)


def calculate_user_groups(tweets: QuerySet) -> Dict:
    firestorm_df = query_set_to_df(tweets)
    print(len(firestorm_df))
    print(firestorm_df.columns)
    # group by author id
    firestorms_user_activity_counts = firestorm_df.groupby(['author_id']).size().reset_index(name='count')

    firestorms_user_activity_counts.sort_values(by=["count"], ascending=False, inplace=True)

    user_groups = {
        'hyper_active_users': list(
            firestorms_user_activity_counts.iloc[:len(firestorms_user_activity_counts) // 100]['author_id']),
        'active_users': list(
            firestorms_user_activity_counts.iloc[
            len(firestorms_user_activity_counts) // 100: len(firestorms_user_activity_counts) // 10]['author_id']),
        'lurking_users': list(
            firestorms_user_activity_counts.iloc[len(firestorms_user_activity_counts) // 10:]['author_id'])
    }

    print(f'{len(user_groups["hyper_active_users"])} hyper_active_users, {len(user_groups["active_users"])}'
          f' active_users and {len(user_groups["lurking_users"])} lurking_users')

    return user_groups

if __name__ == "__main__":
    connect_to_mongo()
    #print(len(get_tweets_for_search_query('pinkygloves')))
    add_attributes_to_tweets(get_tweets_for_search_query('lehman'), ['tweet_type', 'user_type', 'contains_url'])