import pandas as pd
from mongoengine import QuerySet
from json import loads

from timebudget import timebudget

from src.db.queries.tweet_queries import get_tweets_for_search_query, get_tweet_for_id
from src.db.connection import connect_to_mongo
from src.db.queries.tweet_mutations import add_attribute_to_tweet
from src.db.helpers import query_set_to_df
from src.discourse_style_metrics.discourse_stlye_metrics import (
    contains_url,
    user_type,
    tweet_type,
)
from src.discourse_style_metrics.offensiveness_predict import predict_single, load_model
from src.discourse_style_metrics.offensiveness_training import CLASS_LIST
from src.db.queried import QUERIES

from typing import List, Dict, Tuple


def add_attributes_to_tweets(
    tweets: QuerySet, attributes: List[str], overwrite: bool = False
):
    if "user_type" in attributes:
        user_groups = calculate_user_groups(tweets)

    if "is_offensive" in attributes:
        model, tokenizer = load_model(
            "../../models/german_hatespeech_detection_finetuned"
        )

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
                elif attribute == "is_offensive":
                    value = determine_offensiveness(tweet_dict, model, tokenizer)
                else:
                    raise ValueError(
                        f"No known preprocessing operation exists for adding the attribute {attribute}"
                    )

                add_attribute_to_tweet(tweet, attribute, value)


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


def determine_offensiveness(tweet: dict, model, tokenizer):
    if not "tweet_type" in tweet:
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
                        print("Did not find a referenced tweet for this tweet")
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
    query_set = get_tweets_for_search_query(QUERIES["pinkygloves"]["query"])

    print(f"Starting to calculate offensiveness for {len(query_set)} tweets")

    with timebudget(f"Calculating offensiveness for {len(query_set)} tweets"):
        add_attributes_to_tweets(query_set, ["is_offensive"])
