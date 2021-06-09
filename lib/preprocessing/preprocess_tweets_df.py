import pandas as pd
from datetime import datetime
from lib.utils.datetime_helpers import unix_ms_to_date, round_to_hour

TWEET_TYPE_LEVELS = pd.api.types.CategoricalDtype(categories=["retweet with comment", "retweet without comment",
                                                              "reply", "original tweet"])

USER_TYPE_LEVELS = pd.api.types.CategoricalDtype(categories=["laggard", "hyper-active", "active"])


def preprocess_tweets_df(tweets: pd.DataFrame) -> pd.DataFrame:
    """ Parses created_at to datetime, adds hour attributes and casts categorical variables (e.g. tweet type) to
    categorical dytpe

    :param tweets: tweets to preprocess
    :return: preprocessed tweets
    """

    # adding date attributes
    tweets['created_at'] = tweets['created_at'].apply(lambda x: unix_ms_to_date(x['$date']))
    tweets['hour'] = tweets['created_at'].apply(lambda x: round_to_hour(x))

    # casting attributes to categorical
    tweets["tweet_type"] = tweets["tweet_type"].astype(TWEET_TYPE_LEVELS)
    tweets['user_type'] = tweets['user_type'].astype(USER_TYPE_LEVELS)
    tweets['lang'] = tweets['lang'].astype("category")

    return tweets


def select_time_range(tweets: pd.DataFrame, start_point: datetime, end_point: datetime,
                      time_variable: str = 'created_at' ) -> pd.DataFrame:
    """ from a selection of tweets returns only those tweets that lie in the specified time range

    :param tweets: tweets from which you want to select only those in the time range
    :param start_point: starting point (inclusive) of the time range (only tweets after this point are returned)
    :param end_point: end point (exclusive) of the time range (only tweets before this point are returned)
    :param time_variable: time variable by which the selection should happen (defaults to 'created_at', e.g. 'hour'
    migh also make sense in some instances)
    :return: tweets after start_point AND before end_point
    """
    return tweets[(tweets[time_variable] >= start_point) & (tweets[time_variable] < end_point)]