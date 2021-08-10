from lib.twitter_data.tweets import Tweets
from lib.db.queried import QUERIES
from lib.db.connection import connect_to_mongo


def get_firestorms_metadata(query: str):
    """Returns a selection of metadata for the firestorm described by the query
    :param query: the query for the firestorm
    :return: A dictionary containing metadata such as length, average_aggression, start and end data for the firestorm
    """

    firestorm = Tweets.from_query(query)
    print(len(firestorm))

    # print(firestorm.tweets.iloc[0])

if __name__ == "__main__":
    connect_to_mongo()
    get_firestorms_metadata(QUERIES['pinkygloves']['query'])