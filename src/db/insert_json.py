import json
from typing import Dict, List, Tuple

import src.db.schemes as mongo_db
from src.db.connection import connect_to_mongo
from src.db.queries.tweet_queries import get_tweets_for_search_query
from src.discourse_style_metrics.add_attributes_to_entries import (
    add_attributes_to_tweets,
)


def file_reader_generator(filename: str, encoding=None):
    counter = 0
    for row in open(filename, "r", encoding=encoding):
        if counter % 5000 == 0:
            print(f"Read {counter} lines by now. Continuing reading lines...")
        counter += 1
        yield row


def insert_json_lines_file(filename: str, search_params: dict):
    """Expects a json document where each line represents a json doc not a "true" json file"""
    for row in file_reader_generator(filename, encoding="utf-8-sig"):
        try:
            tweet = json.loads(row)
            tweet["search_params"] = search_params
            insert_one_tweet(tweet)
        except Exception as e:
            print(f"Tweet insertion failed: {e}")


def insert_one_tweet(entry: Dict) -> None:
    mongo_db.Tweets.from_json(json.dumps(entry), True).save()


def insert_many_tweets(entries: List[Dict]):
    tweet_db_instances = [
        mongo_db.Tweets.from_json(json.dumps(entry)) for entry in entries
    ]
    mongo_db.Tweets.objects.insert(tweet_db_instances, loadBulk=False)


def clean_json(input_filname: str, output_filename: str) -> Tuple[int, int, int]:
    """From a files containing jsons attempts to remove all unwanted data (not directly containing info on tweets)
    :param input_filname: file to read dirty json from
    :param output_filename: file to write clean json to
    :return: a clean report of the form
    (nr of removed "newest_id", nr of removed "users" lines, nr of removed "media" lines)
    """

    newest_rmd, users_rmd, media_rmd = 0, 0, 0
    with open(output_filename, "w") as output:
        for row in file_reader_generator(input_filname):
            # check if first few chars include bannes words
            first_eight_chars = row[:8]
            if first_eight_chars == '{"newest':
                newest_rmd += 1
            elif first_eight_chars == '{"users"':
                users_rmd += 1
            elif first_eight_chars == '{"media"':
                media_rmd += 1
            else:
                output.write(row)

    return newest_rmd, users_rmd, media_rmd


if __name__ == "__main__":
    connect_to_mongo()

    BASE_URL = "/home/david/Desktop/Masterarbeit/twit_scrape/data/firestorms/"

    # CONFIGURE THESE TWO
    insert_filename = "helmeLookLikeShitFix.json"
    query = "#HelmeRettenLeben OR #lookslikeshit OR #saveslifes OR conversation_id:1108842805089177615"
    #######

    search_params = {"query": query}
    print(len(get_tweets_for_search_query(query)))

    print(f"Cleaning {insert_filename}\n")
    clean_stats = clean_json(
        BASE_URL + "dirty/" + insert_filename, BASE_URL + insert_filename
    )

    print(
        f"**Removed {clean_stats[0]} meta stats, {clean_stats[1]} user stats, {clean_stats[2]} media stats"
    )

    print("\n**Starting to inserts tweets into the db now!")
    insert_json_lines_file(BASE_URL + insert_filename, search_params=search_params)

    print("Done with Inserting!\n")

    print("**Running preprocessing steps (aggression not added)!")
    add_attributes_to_tweets(
        get_tweets_for_search_query(query), ["tweet_type", "user_type", "contains_url"]
    )
