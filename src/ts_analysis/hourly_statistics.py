from src.twitter_data.tweets import Tweets


def tweet_quantity_per_hour(firestorm: Tweets) -> list[int]:
    """Returns the quantity of tweets per hour (only in true_data time range)

    :param firestorm: collection of tweets for which the tweet quantity should be calculated
    :return: list of total_tweets per hour for each hour in firestorm
    """

    return list(firestorm.hourwise_metrics["total_tweets"].astype("int64"))


def offensiveness_per_hour(firestorm: Tweets) -> list[float]:
    """Returns the offensiveness in the discourse of the firestorm per hour to file_path (only in true_data time range)

     :param firestorm: collection of tweets for which the offensiveness should be calculated
    :return: percentage of offensive tweets per hour of the discourse in the queried firestorm (rounded to 2 decimals)
    """
    offensiveness_pct_raw = list(firestorm.hourwise_metrics["offensive_pct"])
    return [round(offensiveness_pct, 4) for offensiveness_pct in offensiveness_pct_raw]
