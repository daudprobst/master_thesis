import json

import pandas as pd
from mongoengine import QuerySet
import os

from src.db.connection import connect_to_mongo
from src.db.schemes import Tweets


def query_set_to_df(input_data: QuerySet) -> pd.DataFrame:
    """Transforms the QuerySet into a DataFrame

    :param input_data: The query set that should be transformed to a DataFrame
    :return: Data as a DataFrame
    """
    return pd.DataFrame.from_records(json.loads(input_data.to_json()))


def get_random_oids(collection, sample_size: int):
    pipeline = [{"$project": {"_id": 1}}, {"$sample": {"size": sample_size}}]
    return [s["_id"] for s in collection.aggregate(pipeline)]


def get_random_documents(DocCls, sample_size: int) -> QuerySet:
    doc_collection = DocCls._get_collection()
    random_oids = get_random_oids(doc_collection, sample_size)
    return DocCls.objects(id__in=random_oids)


def tweet_sample_to_csv(sample_size: int, output_filename: str):
    """A very specific implementation of gathering/filtering steps for collecting a set of sample tweets

    :param sample_size: Number of tweets that should be in the sample
    :param output_filename: Filename to which the sample should be written
    """
    # we get a much larger sample size as we are applying a lot of filtering steps - unsafe implementation
    sample = get_random_documents(Tweets, sample_size * 10).only(
        "text", "tweet_type", "lang", "id"
    )
    sample_df = query_set_to_df(sample)

    # integrating retweet could mean that labelers are shown the same tweet multiple times
    sample_df = sample_df[sample_df["tweet_type"] != "retweet without comment"]
    # some older fetches (not used in thesis) have nan and are kicked out here!
    sample_df = sample_df[sample_df["tweet_type"].notna()]
    # only use german tweets
    sample_df = sample_df[sample_df.lang == "de"]
    # shuffle random sample
    sample_df = sample_df.sample(frac=1).reset_index(drop=True)

    if len(sample_df) < sample_size:
        raise Exception(
            "Method did not produce a big enough sample. Check the filtering steps in the method or your query."
        )

    selected_sample = sample_df.head(sample_size)

    selected_sample = selected_sample[["_id", "text"]]

    selected_sample.to_csv(
        output_filename,
        sep="\t",
        header=["id", "text"],
    )


if __name__ == "__main__":
    connect_to_mongo()
    tweet_sample_to_csv(300, f"{os.getcwd()}/data/aggr_sample/full_sample.csv")
