from lib.twitter_data.tweets_in_phases import TweetsInPhases
from lib.twitter_data.tweets import Tweets
from lib.db.connection import connect_to_mongo
import pandas as pd
from datetime import datetime
from lib.graphs.line_plots import smoothed_line_plots
from typing import Tuple
import pymannkendall
import plotly.express as px
from lib.db.queried import QUERIES as queried_firestorms
from scipy.stats import pearsonr


def test_for_trend(tweet_metrics_by_hour: pd.DataFrame, attribute: str, day_period=24) -> Tuple:
    """ Tests for a trend in the time series

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

    '''
            Seasonal MK Test (seasonal_test): For seasonal time series data, Hirsch, R.M., Slack, J.R. and Smith, R.A. (1982)
            proposed this test to calculate the seasonal trend.
            (https://pypi.org/project/pymannkendall/)

           '''
    return pymannkendall.seasonal_test(tweet_metrics_by_hour[attribute], period=day_period)


if __name__ == "__main__":
    connect_to_mongo()

    # select firestorm
    firestorm_meta = queried_firestorms['pinkygloves']
    ####

    print(f'Analyzing tweets for query "{firestorm_meta["query"]}" between {firestorm_meta["true_start_date"]} '
          f'and {firestorm_meta["true_end_date"]}')

    firestorm_tweets = Tweets.from_hashtag_in_query(firestorm_meta['query']).select_time_range(
        firestorm_meta['true_start_date'], firestorm_meta['true_end_date']
    )

    print(
        list(
            zip(
                list(firestorm_tweets.six_hourwise_metrics['offensive_pct']),
                list(firestorm_tweets.six_hourwise_metrics['total_tweets'])
            )
        )
    )

    '''
    print(firestorm_tweets.six_hourwise_metrics.shape)

    print(
        pearsonr(
            firestorm_tweets.hourwise_metrics['total_tweets'].astype('float64'),
            firestorm_tweets.hourwise_metrics['offensive_pct'].astype('float64')
        )
    )

    print(f'Firestorm has {len(firestorm_tweets)} phases and {len(firestorm_tweets.tweets)} tweets')
    smoothed_line_plots(firestorm_tweets.six_hourwise_metrics,
                        x='six_hour_slot', y=['total_tweets_pct', 'offensive_pct'], #, 'retweet_pct', 'laggards_pct', 'de_pct'],
                        window_size=0).show()


    fig = px.scatter(firestorm_tweets.six_hourwise_metrics, x="six_hour_slot", y="offensive_pct", trendline="ols")
    fig.show()


    # print('The following breakpoints were detected:')
    # print(firestorm_tweets.breakpoints)


    # Testing significance of trends
    for variable in ['offensive_pct']:
        test_statistics = test_for_trend(firestorm_tweets.six_hourwise_metrics, variable)
        print(f'\nTrend test for {variable}:\n')
        print(test_statistics)
        if test_statistics.p > 0.05:
            print('No significant trend!')
        else:
            print('Trend significant!')

    '''

    # Testing significance of trends WITHIN PHASES
    '''
    firestorm_phases_metrics_per_hour = [phase.hourwise_metrics for phase in firestorm_tweets.phases]
        # -> significant trend for laggards_pct, no significance for retweet_pct!


    for i, phase_df in enumerate(firestorm_phases_metrics_per_hour):
        print(f'\n****Testing phase {i}****\n')
        for variable in ['retweet_pct', 'laggards_pct']:
            test_statistics = test_for_trend(phase_df, variable)
            print(f'Trend test for {variable}: {test_statistics}')
            if test_statistics.p > 0.05:
                print('No significant trend!')
            else:
                print('Trend significant!')

        # scatter plot with ols trendline
        # fig = px.scatter(phase_df, x="hour", y="laggards_pct", trendline="ols")
        # fig.show()


        # TEST WHETHER PHASES ARE SIGNIFICANTLY DIFFERENT - ONLY FOR TWO PHASES!

        from scipy.stats import ttest_ind
        from statistics import mean
        test_var = 'laggards_pct'
        print(f'\n ****T-test: Significant differences between retweet_pct in groups? for {test_var}****')
        print(mean(firestorm_phases_metrics_per_hour[0][test_var]))
        print(mean(firestorm_phases_metrics_per_hour[1][test_var]))
        ttest_result = ttest_ind(
            a=firestorm_phases_metrics_per_hour[0][test_var],
            b=firestorm_phases_metrics_per_hour[1][test_var],
            equal_var=False, # TODO check whether we need this or if it can be true
            alternative='two-sided' # TODO adjust! {‘two-sided’, ‘less’, ‘greater’}
        )
        pvalue = ttest_result.pvalue
        if pvalue < 0.05:
            print(f'Significant result with p-value of {pvalue} ')
        else:
            print(f'No significant result with p-value of {pvalue} ')
    '''