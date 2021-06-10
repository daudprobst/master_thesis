from lib.TwitterData.tweets import Tweets
from lib.db.connection import connect_to_mongo
from json import loads
import pandas as pd
from datetime import datetime
from lib.graphs.line_plots import smoothed_line_plots
from lib.preprocessing.preprocess_tweets_df import rates_per_hour, preprocess_tweets_df, select_time_range
from typing import Tuple
import pymannkendall
import plotly.express as px

from src.analysis.change_point_detection import split_tweets_at_breakpoints


def test_for_trend(tweet_metrics_by_hour: pd.DataFrame, attribute: str) -> Tuple:
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
    return pymannkendall.seasonal_test(tweet_metrics_by_hour[attribute], period=24)

if __name__ == "__main__":
    connect_to_mongo()

    firestorm_tweets = Tweets.from_hashtag_in_query('pinkygloves')
    firestorm_tweets = firestorm_tweets.select_time_range(datetime.strptime("2021-04-13 12:00:00", '%Y-%m-%d %H:%M:%S'),
                                     datetime.strptime("2021-04-17 12:00:00", '%Y-%m-%d %H:%M:%S'))

    print(firestorm_tweets)
    # smoothed_line_plots(firestorm_tweets.hourwise_metrics,
    #                    x='hour', y=['total_tweets', 'retweet_pct', 'laggards_pct', 'de_pct']).show()


    for variable in ['retweet_pct', 'laggards_pct']:
        test_statistics = test_for_trend(firestorm_tweets.hourwise_metrics, variable)
        print(f'Trend test for {variable}: {test_statistics}')
        if test_statistics.p > 0.05:
            print('No significant trend!')
        else:
            print('Trend significant!')

    fig = px.scatter(firestorm_tweets.hourwise_metrics, x="hour", y="laggards_pct", trendline="ols")
    fig.show()

    '''
    firestorm_phases = split_tweets_at_breakpoints(firestorm_df)
    firestorm_phases_metrics_per_hour = [rates_per_hour(phase) for phase in firestorm_phases]
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


        # TEST WHETHER PHASES ARE SIGNIFICANTLY DIFFERENT

        from scipy.stats import ttest_ind
        from statistics import mean
        print("T-test for retweets")
        print(mean(firestorm_phases_metrics_per_hour[0]['retweet_pct']))
        print(mean(firestorm_phases_metrics_per_hour[1]['retweet_pct']))
        print(ttest_ind(
            a=firestorm_phases_metrics_per_hour[0]['retweet_pct'],
            b=firestorm_phases_metrics_per_hour[1]['retweet_pct'],
            equal_var=False, # TODO check whether we need this or if it can be true
            alternative='two-sided' # TODO adjust! {‘two-sided’, ‘less’, ‘greater’}
        ))
    '''



    '''
    DECOMPOSING TS
    
    from statsmodels.tsa.seasonal import seasonal_decompose
    import matplotlib.pyplot as plt
    
    decomposed_ts = seasonal_decompose(tweet_metrics_by_hour['retweet_pct'], model='additive', extrapolate_trend='freq')

    plt.rcParams.update({'figure.figsize': (10, 10)})
    decomposed_ts.plot().suptitle('Additive Decompose', fontsize=22)
    plt.show()

    df_reconstructed = pd.concat([decomposed_ts.seasonal, decomposed_ts.trend, decomposed_ts.resid, decomposed_ts.observed], axis=1)
    df_reconstructed.columns = ['seas', 'trend', 'resid', 'actual_values']
    print(df_reconstructed.iloc[15])
    print(df_reconstructed.head())
    '''