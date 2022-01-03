import pandas as pd
from mongoengine.queryset.visitor import Q

from src.db.queried import QUERIES, query_iterator
from src.db.helpers import query_set_to_df
from src.db.schemes import Tweets


def load_hypothesis_dataset(
    attributes: list[str] = ["is_offensive", "tweet_type", "user_type"]
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


def dummify_categorical(
    firestorm_df: pd.DataFrame, variable_name: str, value_to_drop: str = None
) -> pd.DataFrame:

    print("DEF")
    # create dummies
    dummies = pd.get_dummies(firestorm_df[variable_name])

    # drop one dummy var (base category)
    if value_to_drop:
        dummies = dummies.drop(value_to_drop, axis=1)
    else:  # drop first column if nothing else is specified
        dummies = dummies.drop(dummies.columns[1], axis=1)

    firestorm_dummies = pd.concat(
        [firestorm_df, dummies],
        axis=1,
    )

    # drop now dummified categorical
    firestorm_dummies = firestorm_dummies.drop(variable_name, axis=1)

    # replace whacko variable names
    firestorm_dummies.columns = firestorm_dummies.columns.str.replace(" ", "_")
    firestorm_dummies.columns = firestorm_dummies.columns.str.replace("-", "_")

    return firestorm_dummies
