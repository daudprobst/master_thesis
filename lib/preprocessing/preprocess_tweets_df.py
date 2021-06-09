import pandas as pd
from datetime import datetime
from lib.utils.datetime_helpers import unix_ms_to_date, round_to_hour
from typing import Sequence, Tuple

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


def rates_per_hour(tweets: pd.DataFrame, to_calculate: Sequence[Tuple[str, str, str]] = None,
                   grouping_var: str = 'hour') -> pd.DataFrame:
    """ Groups the tweets per hour and calculates percentages for certain values in categorical variables that
    were specified in to_calculate (e.g. ("retweet_pct", 'tweet_type', 'retweet without comment') will include the
    percentage of retweets without comment for each hour in the column 'retweet_pct')

    :param tweets: input tweets
    :param grouping_var: variable by which the grouping should occur (e.g. hour)
    :param to_calculate: variables to calculate for each hour; each output is described by a tuple with three entries:
    1. colname: name that the output column should have (e.g. 'retweet_pct')
    2. variable_name: name of variable in input column (e.g. 'tweet_type')
    3. value: value for which the rate should be calculated (e.g. 'retweet without comment')
    :return: data frame with metrics(rates) for each hour for some variables
    """

    if not to_calculate:
        # TODO is using only n-1 attributes to avoid multicollinearity correct?
        to_calculate = [
            ("total_tweets", None, None),
            # ==tweet type
            ("retweet_pct", 'tweet_type', 'retweet without comment'),
            ("original_tweet_pct", 'tweet_type', 'original tweet'),
            ("reply_pct", 'tweet_type', 'reply'),
            # ("quoted_pct", 'tweet_type', 'retweet with comment'),
            # ==user type
            ("laggards_pct", 'user_type', 'laggard'),
            ("active_pct", 'user_type', 'active'),
            # ("hyper_active_pct", 'user_type', 'hyper-active'),
            # ==lang
            ("de_pct", 'lang', 'de'),
            # ("en_pct", 'lang', 'en'),
        ]

    # setting up the output_df
    df_index = tweets[grouping_var].unique()
    cols = [x[0] for x in to_calculate]
    output_df = pd.DataFrame(columns=cols, index=df_index)

    # group all tweets that appeared in the same hour and calculate stats for them
    tweets_by_hour = tweets.groupby(grouping_var)
    for name, group in tweets_by_hour:
        total_length = len(group)
        for col, var_name, value in to_calculate:
            if col == 'total_tweets':
                output_df.at[name, 'total_tweets'] = total_length
            else:
                output_df.at[name, col] = group[var_name].value_counts()[value] / total_length

    if 'total_tweets' in output_df.columns:
        # Normalizing total length (only works since data has only positive values)
        output_df['total_tweets'] = output_df['total_tweets'] / output_df['total_tweets'].max()

    # sort the output by time (hour)
    return output_df.sort_index()


def entries_per_hour(tweets: pd.DataFrame):
    for i,tweet in tweets.iterrows():
        tweet['created_at'] = unix_ms_to_date(tweet['created_at']["$date"])
        tweets._set_value(i, "hour", round_to_hour(tweet['created_at']))

    print(tweets.iloc[0])
    return tweets.groupby(['hour']).size().reset_index(name='count')