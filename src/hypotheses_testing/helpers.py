import pandas as pd
from mongoengine.queryset.visitor import Q

from src.db.queried import QUERIES
from src.db.helpers import query_set_to_df
from src.db.schemes import Tweets

from sklearn.preprocessing import LabelEncoder


def load_hypothesis_dataset(
    attributes: list[str] = ["is_offensive", "tweet_type", "user_type"]
) -> pd.DataFrame:
    tweets_collection = []
    for name, query_dict in QUERIES.items():
        print(f"Loading {name}")
        query = query_dict["query"]
        firestorm_tweets_query_set = Tweets.objects(
            Q(search_params__query=query) & Q(lang="de")
        ).only(*attributes)

        firestorm_df = query_set_to_df(firestorm_tweets_query_set)

        tweets_collection.append(firestorm_df)

    return pd.concat(tweets_collection)


def dummify_categoricals(firestorm_df: pd.DataFrame) -> pd.DataFrame:
    firestorm_dummies = pd.concat(
        [
            firestorm_df,
            pd.get_dummies(firestorm_df["tweet_type"]),
            pd.get_dummies(firestorm_df["user_type"]),
        ],
        axis=1,
    )

    # drop one dummy var
    firestorm_dummies = firestorm_dummies.drop(
        ["retweet without comment", "laggard"], axis=1
    )

    aggr_enc = LabelEncoder()
    firestorm_dummies["aggression_num"] = aggr_enc.fit_transform(
        firestorm_df["is_offensive"]
    )

    firestorm_dummies = firestorm_dummies.drop(
        ["is_offensive", "tweet_type", "user_type"], axis=1
    )

    firestorm_dummies.columns = firestorm_dummies.columns.str.replace(" ", "_")
    firestorm_dummies.columns = firestorm_dummies.columns.str.replace("-", "_")

    return firestorm_dummies
