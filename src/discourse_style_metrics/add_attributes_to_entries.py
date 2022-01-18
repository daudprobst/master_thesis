import pandas as pd
import numpy as np
from json import loads
from typing import Dict, List

from mongoengine import QuerySet
from timebudget import timebudget

from src.db.connection import connect_to_mongo
from src.db.helpers import query_set_to_df
from src.db.queried import QUERIES, query_iterator
from src.db.tweet_mutations import add_attribute_to_tweet
from src.db.tweet_queries import get_tweet_for_id, get_tweets_for_search_query
from src.discourse_style_metrics.discourse_stlye_metrics import (
    contains_url,
    tweet_type,
    user_type,
)
from src.discourse_style_metrics.offensiveness_predict import load_model, predict_single
from src.discourse_style_metrics.offensiveness_training import CLASS_LIST


FIVETEEN_MINS_IN_MS = 900000
HOUR_IN__MS = 3600000


def fs_activity_over_time(query_set: QuerySet):
    tweets_df = query_set_to_df(query_set)
    tweets_df["timestamp"] = tweets_df.apply(lambda x: x["created_at"]["$date"], axis=1)
    tweets_df.plot.line("timestamp", "firestorm_activity")


def add_attributes_to_tweets(
    tweets: QuerySet, attributes: List[str], overwrite: bool = False
):
    if "user_type" in attributes:
        user_groups = calculate_user_groups(tweets)

    if "user_activity" in attributes:
        activities_for_author = activity_per_user(tweets)

    if "is_offensive" in attributes:
        model, tokenizer = load_model("/models/german_hatespeech_detection_finetuned")

    if "firestorm_activity" in attributes:
        timestamps_list = fs_timestamps(tweets)

    if "firestorm_activity_rel" in attributes:
        firestorm_df = query_set_to_df(tweets)
        max_activity = firestorm_df["firestorm_activity"].max()
        print(f"Comparing activity against max of {max_activity}")

    for i, tweet in enumerate(tweets):
        tweet_dict = loads(tweet.to_json())
        if i % 1000 == 0:
            print(f"Added attributes for {i} tweets. Continuing...")
        for attribute in attributes:
            if overwrite or attribute not in tweet_dict:
                if attribute == "tweet_type":
                    value = tweet_type(tweet_dict)
                elif attribute == "contains_url":
                    value = contains_url(tweet_dict)
                elif attribute == "user_type":
                    value = user_type(tweet_dict, user_groups)
                elif attribute == "user_activity":
                    value = activities_for_author[tweet_dict["author_id"]]
                elif attribute == "firestorm_activity":
                    value = fs_activity(
                        tweet_dict["created_at"]["$date"],
                        timestamps_list,
                        HOUR_IN__MS,
                    )
                elif attribute == "firestorm_activity_rel":
                    value = tweet_dict["firestorm_activity"] / max_activity
                elif attribute == "is_offensive":
                    value = determine_offensiveness(tweet_dict, model, tokenizer)
                else:
                    raise ValueError(
                        f"No known preprocessing operation exists for adding the attribute {attribute}"
                    )

                add_attribute_to_tweet(tweet, attribute, value)


def activity_per_user(tweets: QuerySet) -> pd.DataFrame:
    firestorm_df = query_set_to_df(tweets)
    firestorms_user_activity_counts = (
        firestorm_df.groupby(["author_id"]).size().reset_index(name="count")
    )[["author_id", "count"]]

    firestorms_user_activity_counts.set_index("author_id", inplace=True)

    return firestorms_user_activity_counts.to_dict()["count"]


def calculate_user_groups(tweets: QuerySet) -> Dict:
    firestorm_df = query_set_to_df(tweets)
    print(len(firestorm_df))
    print(firestorm_df.columns)
    # group by author id
    firestorms_user_activity_counts = (
        firestorm_df.groupby(["author_id"]).size().reset_index(name="count")
    )

    firestorms_user_activity_counts.sort_values(
        by=["count"], ascending=False, inplace=True
    )

    user_groups = {
        "hyper_active_users": list(
            firestorms_user_activity_counts.iloc[
                : len(firestorms_user_activity_counts) // 100
            ]["author_id"]
        ),
        "active_users": list(
            firestorms_user_activity_counts.iloc[
                len(firestorms_user_activity_counts)
                // 100 : len(firestorms_user_activity_counts)
                // 10
            ]["author_id"]
        ),
        "lurking_users": list(
            firestorms_user_activity_counts.iloc[
                len(firestorms_user_activity_counts) // 10 :
            ]["author_id"]
        ),
    }

    print(
        f'{len(user_groups["hyper_active_users"])} hyper_active_users, {len(user_groups["active_users"])}'
        f' active_users and {len(user_groups["lurking_users"])} lurking_users'
    )

    return user_groups


""" def fs_activity(entry_timestamp: int, all_timestamps: list[int], time_span: int = FIVETEEN_MINS_IN_MS):
    return len(
        list(
            filter(
                lambda timestamp: timestamp < entry_timestamp
                and timestamp > (entry_timestamp - time_span),
                all_timestamps,
            )
        )
    )
     """


def fs_activity(entry_timestamp: int, all_timestamps: np.array, time_span: int):
    return len(
        all_timestamps[
            (all_timestamps < entry_timestamp)
            & (all_timestamps > (entry_timestamp - time_span))
        ]
    )


def normalized_fs_activity(firestorm_query_set: QuerySet):
    firestorm_df = query_set_to_df(firestorm_query_set)
    firestorm_df


def fs_timestamps(tweets: QuerySet) -> np.array:
    tweets_df = query_set_to_df(tweets)
    return np.array([entry["$date"] for entry in tweets_df["created_at"]])


def determine_offensiveness(tweet: dict, model, tokenizer):
    if "tweet_type" not in tweet:
        raise Exception(
            "The tweet does not have a tweet_type. Tweet type must be calculated for"
            "tweets before making a prediction for their offensiveness."
        )

    # we only predict german tweets
    if tweet["lang"] != "de":
        return None
    else:
        if tweet["tweet_type"] == "retweet without comment":
            if len(tweet["referenced_tweets"]) > 1:
                print("\ntweet with multiple references")
                print(tweet)
            for referenced_tweet in tweet["referenced_tweets"]:
                if referenced_tweet["type"] == "retweeted":
                    retweeted_tweet_id = referenced_tweet["id"]
                    try:
                        tweet_txt = loads(
                            get_tweet_for_id(retweeted_tweet_id).to_json()
                        )["text"]
                    except Exception:
                        # dirty fall back - however this occures very rarely
                        print(
                            "Did not find a referenced tweet for this tweet. Falling back to the original tweet text."
                        )
                        tweet_txt = tweet["text"]
                    break
        else:
            # for quoted tweets and replies,we only look at what the user wrote themselve
            # and ignore the tweet that they quoted or replied to for now
            tweet_txt = tweet["text"]

        tweet_txt = tweet_txt.replace("\n", "|LBR|")
        prediction = CLASS_LIST[predict_single(model, tokenizer, tweet_txt)]
        return prediction == "OFFENSE"


if __name__ == "__main__":
    connect_to_mongo()
    """ 
    for key, query_dict in query_iterator(QUERIES):
        query_set = get_tweets_for_search_query(query_dict["query"])
        with timebudget(f"Calculating firestorm activity percentage for {len(query_set)} tweets"):
            add_attributes_to_tweets(query_set, ["firestorm_activity_rel"]) """

    query_set = get_tweets_for_search_query(QUERIES["sarahlee"]["query"])
    with timebudget(
        f"Calculating firestorm activity percentage for {len(query_set)} tweets"
    ):
        add_attributes_to_tweets(query_set, ["firestorm_activity_rel"])
