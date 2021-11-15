from lib.db.schemes import Tweets
from lib.db.connection import connect_to_mongo
from lib.db.helpers import query_set_to_df
from pathlib import Path


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
    sample_df = sample_df[sample_df["tweet_type"] != "retweet without comment"]
    sample_df = sample_df[
        sample_df["tweet_type"].notna()
    ]  # some older fetches have nan!
    print(len(sample_df))

    sample_df = sample_df[sample_df.lang == "de"]
    print(len(sample_df))

    filtered_sample = sample_df.head(300)
    print(len(filtered_sample))

    from sklearn.utils import shuffle

    # filtered_sample = shuffle(filtered_sample)

    filtered_sample = filtered_sample[["_id", "text"]]

    filtered_sample.to_csv(
        Path("../../../data/aggr_sample/full_sample.csv").resolve(),
        sep="\t",
        header=["id", "text"],
    )
