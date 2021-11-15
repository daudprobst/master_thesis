import lib.db.schemes as mongo_db

import csv
import os
from json import dumps
from typing import Sequence, Dict, List, Union


class TwitterSearchResponse:
    """Object that contains a response for a recent_search request on the Twitter API"""

    def __init__(self, response_dict: Dict, search_params: Dict) -> None:
        self._data = response_dict
        self._search_params = search_params

    @property
    def search_params(self) -> Dict:
        return self._search_params

    @property
    def meta(self) -> Union[Dict, None]:
        try:
            return self._data["meta"]
        except KeyError:
            return None

    @property
    def next_token(self) -> Union[str, None]:
        try:
            return self.meta["next_token"]
        except KeyError:
            return None

    @property
    def media(self) -> Union[List[Dict], None]:
        try:
            return self._data["includes"]["media"]
        except KeyError:
            return None

    @property
    def tweets(self) -> Union[List[Dict], None]:
        try:
            return self._data["data"]
        except KeyError:
            return None

    @property
    def users(self) -> Union[List[Dict], None]:
        try:
            return self._data["includes"]["users"]
        except KeyError:
            print("No users found!")
            return None

    def attach_media_to_tweets(self) -> Union[List[Dict], None]:
        if not self.media:
            return self.tweets

        for media in self.media:
            for tweet in self.tweets:
                if "attachments" in tweet and "media_keys" in tweet["attachments"]:
                    if media["media_key"] in tweet["attachments"]["media_keys"]:
                        tweet["media"] = media

        return self.tweets

    def write_to_db(self) -> None:
        for entry in self.attach_media_to_tweets():
            entry["search_params"] = self.search_params
            mongo_db.Tweets.from_json(
                dumps(entry), True
            ).save()  # TODO force_insert=True?

        for user in self.users:
            mongo_db.Users.from_json(dumps(user), True).save()  # updates existing users

    def write_to_csv(
        self,
        filename: str = "/home/david/Desktop/Masterarbeit/twit_scrape/data/firestorms.csv",
    ) -> None:
        """
        Writes Tweet data (without user includes) to a specified csv file
        :param filename: full filepath specified for the .csv
        """
        self.buffer_missing_fields()
        # ordered dict work in Python 3.7+
        response_data_sorted = [dict(sorted(row.items())) for row in self.tweets]
        if not os.path.exists(filename):
            print(
                f"File with filepath {filename} was not found. Reinitializing with empty file and headers."
            )
            with open(filename, "w", newline="") as csvfile:
                spamwriter = csv.writer(csvfile)
                spamwriter.writerow(response_data_sorted[0])

        with open(filename, "a", newline="") as csvfile:
            for row in response_data_sorted:
                spamwriter = csv.writer(csvfile)
                spamwriter.writerow(row.values())

    def buffer_missing_fields(self) -> Sequence[Dict]:
        """Buffers requested fields for which TwitterAPI returned no answer (e.g. often the case for 'withheld'
        for with an empty string. This happens in place for the self.tweets of the instance"""
        for tweet in self.tweets:
            for field in self._search_params["tweet.fields"].split(","):
                if field not in tweet:
                    tweet[field] = ""
            # We manually add media with attach_media_to_tweets()
            if "media" not in tweet:
                tweet["media"] = ""
        return self.tweets
