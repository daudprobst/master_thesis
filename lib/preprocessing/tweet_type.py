
def tweet_type(tweet: dict) -> str:
    """ Returns the type of the tweet

    :param tweet: Full tweet object
    :return: Type of tweet ('retweet with comment', 'retweet without comment', 'reply', or 'original tweet)
    """

    if not tweet['referenced_tweets']:
        return 'original tweet'

    # TODO sometimes there are multiple referenced tweets like
    #  [{'id': 1392103648059080705, 'type': 'quoted'}, {'id': 1393088697713709057, 'type': 'replied_to'}]
    #  how should we treat these cases? For now we simply focus on the first tweet
    reference_type = tweet['referenced_tweets'][0]['type']

    if reference_type == 'quoted':
        return 'retweet with comment'
    elif reference_type == 'retweeted':
        return "retweet without comment"
    elif reference_type == 'replied_to':
        return "reply"
    else:  # somehow there is an unknown reference type
        raise Exception(f"Unknown tweet type with reference type {reference_type} occurred")
