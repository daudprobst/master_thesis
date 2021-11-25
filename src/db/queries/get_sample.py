import os

from src.db.connection import connect_to_mongo
from src.db.helpers import query_set_to_df
from src.db.schemes import Tweets


def get_random_oids(collection, sample_size):
    pipeline = [{"$project": {"_id": 1}}, {"$sample": {"size": sample_size}}]
    return [s["_id"] for s in collection.aggregate(pipeline)]


def get_random_documents(DocCls, sample_size):
    doc_collection = DocCls._get_collection()
    random_oids = get_random_oids(doc_collection, sample_size)
    return DocCls.objects(id__in=random_oids)


if __name__ == "__main__":
    connect_to_mongo()
    sample = get_random_documents(Tweets, 3000).only("text", "tweet_type", "lang", "id")

    sample_df = query_set_to_df(sample)
    print(len(sample_df))
    # integrating retweet could mean that labelers are shown the same tweet multiple times
    sample_df = sample_df[sample_df["tweet_type"] != "retweet without comment"]
    sample_df = sample_df[
        sample_df["tweet_type"].notna()
    ]  # some older fetches (not used in thesis) have nan and are kicked out here!
    sample_df = sample_df[
        sample_df["tweet_type"].notna()
    ]  # some older fetches (not used in thesis) have nan and are kicked out here!

    print(len(sample_df))

    # only use german tweets
    sample_df = sample_df[sample_df.lang == "de"]

    # shuffle random sample
    sample_df = sample_df.sample(frac=1).reset_index(drop=True)

    print(len(sample_df))
    selected_sample = sample_df.head(300)
    print(len(selected_sample))

    selected_sample = selected_sample[["_id", "text"]]

    selected_sample.to_csv(
        f"{os.getcwd()}/data/aggr_sample/full_sample.csv",
        sep="\t",
        header=["id", "text"],
    )
