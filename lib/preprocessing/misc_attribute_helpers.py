
def contains_url(tweet: dict) -> bool:
    """Returns whether the tweet contains an url"""
    return 'urls' in tweet['entities']
