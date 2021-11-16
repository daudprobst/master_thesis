from typing import Sequence, Tuple

import pandas as pd
import pymannkendall

from src.db.connection import connect_to_mongo
from src.db.queried import QUERIES as queried_firestorms
from src.twitter_data.tweets import Tweets


def log_trend_tests(tweets: Tweets, variables_to_test: Sequence[str]):
    for variable in variables_to_test:
        test_statistics = test_for_trend(tweets.six_hourwise_metrics, variable)
        print(f"\nTrend test for {variable}:\n")
        print(test_statistics)
        if test_statistics.p > 0.05:
            print("No significant trend!")
        else:
            print("Trend significant!")


def test_for_trend(
    tweet_metrics_by_hour: pd.DataFrame, attribute: str, day_period=24
) -> Tuple:
    """Tests for a trend in the time series

    :param tweet_metrics_by_hour:
    :param attribute: attribute for which the trend should be tested (e.g. 'retweet_pct')
    :return:  And all Mann-Kendall tests return a named tuple which contained:

            trend: tells the trend (increasing, decreasing or no trend)
            h: True (if trend is present) or False (if the trend is absence)
            p: p-value of the significance test
            z: normalized test statistics
            Tau: Kendall Tau
            s: Mann-Kendal's score
            var_s: Variance S
            slope: Theil-Sen estimator/slope
            intercept: intercept of Kendall-Theil Robust Line, for seasonal test, full period cycle consider as unit time step

        sen's slope function required data vector. seasonal sen's slope also has optional input period, which by the default value is 12. Both sen's slope function return only slope value.
    """

    """
            Seasonal MK Test (seasonal_test): For seasonal time series data, Hirsch, R.M., Slack, J.R. and Smith, R.A. (1982)
            proposed this test to calculate the seasonal trend.
            (https://pypi.org/project/pymannkendall/)

           """
    return pymannkendall.seasonal_test(
        tweet_metrics_by_hour[attribute], period=day_period
    )


if __name__ == "__main__":
    connect_to_mongo()

    # select firestorm
    key = "laschetLacht"
    firestorm_meta = queried_firestorms[key]
    ####

    # for key, firestorm_meta in queried_firestorms.items():
    """
    print(
        f'Analyzing tweets for query "{firestorm_meta["query"]}" between {firestorm_meta["data_start_date"]} '
        f'and {firestorm_meta["data_end_date"]}'
    )
    firestorm_tweets = Tweets.from_query(firestorm_meta["query"]).select_time_range(
        firestorm_meta["data_start_date"], firestorm_meta["data_end_date"]
    )
    firestorm_tweets.plot_quantity_per_hour(title=key)
    """
    """
    
    print(
        list(
            zip(
                list(firestorm_tweets.six_hourwise_metrics['offensive_pct']),
                list(firestorm_tweets.six_hourwise_metrics['total_tweets'])
            )
        )
    )
    """

    """

    print(
        pearsonr(
            firestorm_tweets.hourwise_metrics['total_tweets'].astype('float64'),
            firestorm_tweets.hourwise_metrics['offensive_pct'].astype('float64')
        )
    )

    fig = px.scatter(firestorm_tweets.six_hourwise_metrics, x="six_hour_slot", y="offensive_pct", trendline="ols")
    fig.show()
    """
