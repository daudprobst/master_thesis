from src.db.connection import connect_to_mongo
from datetime import datetime
from src.graphs.pie_plot import pie_plot_multiplot
from src.twitter_data.tweets_in_phases import TweetsInPhases

if __name__ == "__main__":
    connect_to_mongo()

    # Opt: Only using tweets from specified time range (en wave happens between 17th and 23!)
    firestorm_tweets = TweetsInPhases.from_query(
        "pinkygloves", full_match_required=False
    )

    print(
        f"Before selecting the timerange, there are {len(firestorm_tweets.tweets)} in the dataset"
    )

    firestorm_tweets = firestorm_tweets.select_time_range(
        datetime.strptime("2021-04-13 12:00:00", "%Y-%m-%d %H:%M:%S"),
        datetime.strptime("2021-04-17 12:00:00", "%Y-%m-%d %H:%M:%S"),
    )

    print(
        f"After selecting the timerange, there are {len(firestorm_tweets.tweets)} in the dataset"
    )

    from tslearn.clustering import TimeSeriesKMeans
    from tslearn.utils import to_time_series

    ts_hourwise = to_time_series(firestorm_tweets.hourwise_metrics["retweet_pct"])
    print(ts_hourwise.shape)

    km = TimeSeriesKMeans(n_clusters=3, metric="dtw")
    res = km.fit(ts_hourwise)

    # phases_tweets = [phase.tweets for phase in firestorm_tweets.phases]
    # Plotting!
    # pie_plot_multiplot(phases_tweets, attributes_to_plot=['tweet_type', 'user_type', 'lang']).show()
