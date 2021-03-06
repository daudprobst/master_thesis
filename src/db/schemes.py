from datetime import datetime

from mongoengine import (
    Document,
    EmbeddedDocument,
    LongField,
    StringField,
    IntField,
    URLField,
    DictField,
    DateTimeField,
    EmbeddedDocumentField,
    ReferenceField,
    DynamicField,
    BooleanField,
    ListField,
    FloatField,
)


class TweetReference(EmbeddedDocument):
    id = LongField()
    type = StringField()


class TwitterMedia(EmbeddedDocument):
    media_key = StringField(required=True)
    duration_ms = IntField()
    height = IntField()
    width = IntField()
    type = StringField()
    url = URLField()
    preview_image_url = URLField()
    public_metrics = DictField()


class TweetPublicMetrics(EmbeddedDocument):
    retweet_count = IntField()
    reply_count = IntField()
    like_count = IntField()
    quote_count = IntField()


class Tweets(Document):
    id = LongField(primary_key=True)
    text = StringField(required=True)
    created_at = DateTimeField(required=True)
    author_id = ReferenceField("Users")
    conversation_id = LongField()
    possibly_sensitive = BooleanField()
    withheld = BooleanField(default=False)
    reply_settings = StringField()
    in_reply_to_user_id = StringField()
    source = StringField()
    lang = StringField()
    public_metrics = EmbeddedDocumentField("TweetPublicMetrics")
    context_annotations = ListField(DictField())
    entities = DictField()
    attachments = DictField()  # make more specific?
    geo = DynamicField()
    referenced_tweets = ListField(EmbeddedDocumentField("TweetReference"))
    media = EmbeddedDocumentField(
        "TwitterMedia"
    )  # we store media directly in the tweet
    search_params = DictField()
    tweet_type = StringField()
    contains_url = BooleanField()
    user_type = StringField()
    user_activity = IntField()
    firestorm_activity = IntField()
    firestorm_activity_rel = FloatField(min_value=0, max_value=1)
    fetch_date = DateTimeField(default=datetime.utcnow)
    is_offensive = BooleanField()
    includes_users = DictField()
    includes_media = ListField()
    includes_tweets = DictField()


class Users(Document):
    id = LongField(primary_key=True, required=True)
    username = StringField()
    name = StringField()
    verified = BooleanField()
    description = StringField()
    pinned_tweet_id = LongField()
    entities = DynamicField()
    location = StringField()
    created_at = DateTimeField()
    profile_image_url = URLField()
    url = URLField()
    public_metrics = DictField()
    protected = BooleanField()
    withheld = BooleanField()
