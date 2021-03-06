from datetime import datetime, timedelta, timezone

from src.db.connection import connect_to_mongo
from src.twitter_fetching.recent_search import fetch_report_to_csv, recent_search
from src.twitter_fetching.requests_base import create_params
from src.utils.datetime_helpers import day_wrapping_datetimes

if __name__ == "__main__":
    connect_to_mongo()

    a_while_ago = datetime.now(timezone.utc) - timedelta(days=1)

    day_time_stamps = day_wrapping_datetimes(a_while_ago)
    req_field = [
        "attachments",
        "author_id",
        "conversation_id",
        "created_at",
        "entities",
        "geo",
        "id",
        "in_reply_to_user_id",
        "lang",
        "public_metrics",
        "possibly_sensitive",
        "referenced_tweets",
        "source",
        "text",
    ]
    req_user_fields = [
        "id",
        "username",
        "withheld",
        "location",
        "verified",
        "public_metrics",
    ]
    req_media_fields = ["media_key", "type", "url", "public_metrics"]

    req_params = create_params(
        query="#studierenwieBaerbock",
        fields=req_field,
        user_fields=req_user_fields,
        media_fields=req_media_fields,
        start_time=day_time_stamps[0],
        end_time=day_time_stamps[1],
    )

    res_fetch_report = recent_search(
        req_params, tweet_fetch_limit=25000
    )  # tweak fetch limit!
    print(res_fetch_report)
    fetch_report_to_csv(res_fetch_report, "../../data/fetch_log.csv")
