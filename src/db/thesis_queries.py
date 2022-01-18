import pandas as pd
from mongoengine.queryset.visitor import Q
from src.db.helpers import query_set_to_df
from src.db.queried import QUERIES, query_iterator
from src.db.schemes import Tweets


def load_hypothesis_dataset(
    attributes: list[str] = [
        "is_offensive",
        "tweet_type",
        "user_type",
        "user_activity",
        "firestorm_activity_rel",
        "firestorm_activity",
    ]
) -> pd.DataFrame:
    tweets_collection = []
    for _, query_dict in query_iterator(QUERIES):
        query = query_dict["query"]
        firestorm_tweets_query_set = Tweets.objects(
            Q(search_params__query=query) & Q(lang="de")
        ).only(*attributes)

        firestorm_df = query_set_to_df(firestorm_tweets_query_set)

        tweets_collection.append(firestorm_df)

    return pd.concat(tweets_collection)


def load_firestorms_individually(
    attributes: list[str] = [
        "is_offensive",
        "tweet_type",
        "user_type",
        "user_activity",
        "firestorm_activity_rel",
        "created_at",
    ]
) -> pd.DataFrame:
    fs_collection = {}
    for fs_name, query_dict in query_iterator(QUERIES):
        query = query_dict["query"]
        firestorm_tweets_query_set = Tweets.objects(
            Q(search_params__query=query) & Q(lang="de")
        ).only(*attributes)

        firestorm_df = query_set_to_df(firestorm_tweets_query_set)

        fs_collection[fs_name] = firestorm_df

    return fs_collection
