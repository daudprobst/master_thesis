import json
import lib.db.schemes as mongo_db
from lib.db.connection import connect_to_mongo
from typing import List, Dict
import ijson
from time import sleep

def file_reader_generator(filename: str):
    counter = 0
    for row in open(filename, "r"):
        if counter % 500 == 0:
            print(f'Read {counter} lines by now. Continuing reading lines...')
        counter +=1
        yield row

def insert_json_lines_file(filename: str, search_params: dict):
    """Expects a json document where each line represents a json doc not a "tru" json file"""
    for row in file_reader_generator(filename):
        try:
            tweet = json.loads(row)
            tweet['search_params'] = search_params
            insert_one_tweet(tweet)
        except Exception as e:
            print(f'Tweet insertion failed {e}')

def insert_one_tweet(entry: Dict) -> None:
    mongo_db.Tweets.from_json(json.dumps(entry), True).save() # TODO force_insert=True?


def insert_many_tweets(entries: List[Dict]):
    tweet_db_instances = [mongo_db.Tweets.from_json(json.dumps(entry)) for entry in entries]
    mongo_db.Tweets.objects.insert(tweet_db_instances, loadBulk=False)


if __name__ == '__main__':
    connect_to_mongo()
    insert_json_lines_file('/home/david/Desktop/Masterarbeit/twit_scrape/data/pinky_tweets.json',
                           search_params={'query': '#pinkygloves OR #pinkygate'})