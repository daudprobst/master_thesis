import pytz
from json import loads
from typing import Sequence, Tuple

import pandas as pd

from src.db.queries.tweet_queries import get_tweets_for_search_query
from src.graphs.line_plots import df_smoothed_line_plots
from src.utils.datetime_helpers import (
    round_to_hour,
    round_to_hour_slots,
    unix_ms_to_utc_date,
)
from src.utils.conversions import float_to_pct


class Tweets:
    def __init__(self, tweets: pd.DataFrame, filters=[]):
        self._tweets = self._preprocess_inputs(tweets)

        self._filter_log = [len(self._tweets)]
        for filter in filters:
            self._tweets = filter(self._tweets)
            self._filter_log.append(len(self._tweets))

    def __len__(self):
        return len(self.tweets)

    @classmethod
    def from_query(
        cls,
        fetch_query: str,
        filters=[],
        full_match_required: bool = True,
    ):
        """Returns all tweets that were fetched by making use of the specified query

        :param fetch_query: the fetch_query for which tweets should be returned (query originally used for fetching!)
        :param full_match_required: if false, tweets are returned if they were fetched with fetch_query
        if true the fetch_query must be fully equal to the query used for fetching the tweets
        :return: all tweets who were fetched with fetch_query
        """
        firestorm_tweets_selection = loads(
            get_tweets_for_search_query(
                fetch_query, full_match_required=full_match_required
            ).to_json()
        )

        return cls(
            pd.DataFrame.from_records(firestorm_tweets_selection), filters=filters
        )

    @property
    def tweets(self):
        return self._tweets

    @property
    def hourwise_metrics(self):
        if not hasattr(self, "_hourwise_metrics"):
            self._hourwise_metrics = self.metrics_per_time_intervall("hour")
        return self._hourwise_metrics

    @property
    def six_hourwise_metrics(self):
        if not hasattr(self, "_six_hourwise_metrics"):
            self._six_hourwise_metrics = self.metrics_per_time_intervall(
                "six_hour_slot"
            )
        return self._six_hourwise_metrics

    @property
    def filter_log(self):
        return self._filter_log

    def _preprocess_inputs(self, tweets: pd.DataFrame) -> pd.DataFrame:
        """Parses created_at to datetime, adds hour attributes and casts categorical variables (e.g. tweet type) to
        categorical dytpe

        :param tweets: tweets to preprocess
        :return: preprocessed tweets
        """
        TWEET_TYPE_LEVELS = pd.api.types.CategoricalDtype(
            categories=[
                "retweet with comment",
                "retweet without comment",
                "reply",
                "original tweet",
            ]
        )

        USER_TYPE_LEVELS = pd.api.types.CategoricalDtype(
            categories=["laggard", "hyper-active", "active"]
        )

        # adding date attributes
        try:
            tweets["created_at"] = tweets["created_at"].apply(
                lambda x: unix_ms_to_utc_date(x["$date"])
            )
            ger_tz = pytz.timezone("Europe/Berlin")
            tweets["created_at"] = tweets["created_at"].apply(
                lambda x: x.astimezone(ger_tz)
            )
            tweets["created_at"] = tweets["created_at"]
            tweets["hour"] = tweets["created_at"].apply(lambda x: round_to_hour(x))
            tweets["six_hour_slot"] = tweets["created_at"].apply(
                lambda x: round_to_hour_slots(x)
            )
        except Exception:
            # dirty solution: the upper code fails if we had already parsed the dates before
            pass

        # casting attributes to categorical
        if str(tweets.dtypes["tweet_type"]) != "category":
            tweets["tweet_type"] = tweets["tweet_type"].astype(TWEET_TYPE_LEVELS)
        if str(tweets.dtypes["user_type"]) != "category":
            tweets["user_type"] = tweets["user_type"].astype(USER_TYPE_LEVELS)
        if str(tweets.dtypes["lang"]) != "category":
            tweets["lang"] = tweets["lang"].astype("category")

        return tweets

    def metrics_per_time_intervall(
        self,
        grouping_var: str = "hour",
        to_calculate: Sequence[Tuple[str, str, str]] = None,
    ) -> pd.DataFrame:
        """Groups the tweets per hour and calculates percentages for certain values in categorical variables that
        were specified in to_calculate (e.g. ("retweet_pct", 'tweet_type', 'retweet without comment') will include the
        percentage of retweets without comment for each hour in the column 'retweet_pct')

        :param grouping_var: variable by which the grouping should occur (e.g. hour)
        :param to_calculate: variables to calculate for each hour; each output is described by a tuple with three entries:
        1. colname: name that the output column should have (e.g. 'retweet_pct')
        2. variable_name: name of variable in input column (e.g. 'tweet_type')
        3. value: value for which the rate should be calculated (e.g. 'retweet without comment')
        :return: data frame with metrics(rates) for each hour for some variables
        """

        if not to_calculate:
            to_calculate = [
                # ==tweet type
                ("retweet_pct", "tweet_type", "retweet without comment"),
                ("original_tweet_pct", "tweet_type", "original tweet"),
                ("reply_pct", "tweet_type", "reply"),
                ("quoted_pct", "tweet_type", "retweet with comment"),
                # ==user type
                ("laggards_pct", "user_type", "laggard"),
                ("active_pct", "user_type", "active"),
                ("hyper_active_pct", "user_type", "hyper-active"),
                # ==lang
                ("de_pct", "lang", "de"),
                ("en_pct", "lang", "en"),
                # ==offensiveness
                (
                    "offensive_pct",
                    "is_offensive",
                    True,
                ),  # CAREFUL WITH THE NONE VALUES - i.e. not all value are labeled as offensive/not offensive!
                (
                    "not_offensive_pct",
                    "is_offensive",
                    False,
                ),  # FOR TESTING AROUND WITH NONE VALUES
            ]

        # setting up the output_df
        # index should contain hour (not just hours were tweets occured)
        first_timestamp = self.tweets[grouping_var].min()
        last_timestamp = self.tweets[grouping_var].max()
        hourly_timestamps = pd.date_range(first_timestamp, last_timestamp, freq="h")

        cols = [x[0] for x in to_calculate]
        cols.extend(["total_tweets", "total_tweets_pct"])
        output_df = pd.DataFrame(columns=cols, index=hourly_timestamps)
        output_df[
            "hour"
        ] = output_df.index  # also have index as column (often useful for plotting)

        # intialize empty df with 0s
        output_df = output_df.fillna(
            0.0
        )  # we need 0.0 so the col.dtype is float instead of int

        # group all tweets that appeared in the same hour and calculate stats for them
        tweets_by_hour = self.tweets.groupby(grouping_var)

        for name, group in tweets_by_hour:
            total_length = len(group)
            output_df.at[name, "total_tweets"] = total_length
            for col, var_name, value in to_calculate:
                try:
                    output_df.at[name, col] = (
                        group[var_name].value_counts()[value] / total_length
                    )
                except KeyError:  # sometimes value does not exist in value_counts (0 entries)
                    output_df.at[name, col] = 0

        # Normalizing total length (only works since data has only positive values)
        output_df["total_tweets_pct"] = (
            output_df["total_tweets"] / output_df["total_tweets"].max()
        )

        # sort the output by time (hour)
        return output_df.sort_index()

    def plot_quantity_per_hour(self, **kwargs) -> None:
        df_smoothed_line_plots(
            self.hourwise_metrics, x_attr="hour", y_attrs=["total_tweets"], **kwargs
        ).show()

    def metadata(self) -> dict:
        """Returns a selection of metadata for the firestorm described by the query
        :return: A dictionary containing metadata such as length, average_aggression, start and end data for the firestorm
        """

        value_counts = self.tweets.is_offensive.value_counts().to_dict()

        return {
            "length": len(self),
            "filtering_lengths_log": self.filter_log,
            "pct_filtered": float_to_pct(
                1 - (self.filter_log[-1] / self.filter_log[0])
            ),
            "average_aggressiveness": float_to_pct(
                value_counts[True] / (value_counts[True] + value_counts[False])
            ),
            "aggr_value_counts": value_counts,
        }


if __name__ == "__main__":
    from src.db.connection import connect_to_mongo
    from src.db.queried import QUERIES
    from src.twitter_data.filters import default_filters_factory

    connect_to_mongo()
    query_dict = QUERIES["pinkygloves"]
    firestorm = Tweets.from_query(
        query_dict["query"],
        filters=default_filters_factory(query_dict),
    )

    firestorm_df = firestorm.tweets[
        ["is_offensive", "tweet_type", "text", "_id", "user_type"]
    ]

    print("=====AGGRESSION BY TWEET TYPE")
    firestorm_contingency_table = pd.crosstab(
        firestorm_df["is_offensive"], firestorm_df["tweet_type"]
    )
    print(firestorm_contingency_table)


"""
    print(firestorm_df.head(5))
    type_groups = firestorm_df.groupby('tweet_type')

    print(type_groups['is_offensive'].describe())
    for name, group_df in type_groups:
        print("====")
        print(name)
        print(len(group_df))
        print(group_df['is_offensive'].value_counts())
        
        
    laggards_tweets = firestorm_df[firestorm_df['user_type'] == 'laggard']
    
    laggards_grouped = laggards_tweets.groupby('tweet_type')
    
    print(laggards_grouped['is_offensive'].describe())
    
    for name, group_df in laggards_grouped:
        print("====")
        print(name)
        print(len(group_df))
        print(group_df['is_offensive'].value_counts()) """
