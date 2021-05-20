from mongoengine import *
from datetime import datetime


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
    author_id = ReferenceField('Users')
    conversation_id = LongField()
    possibly_sensitive = BooleanField()
    withheld = BooleanField(default=False)
    reply_settings = StringField()
    in_reply_to_user_id = StringField()
    source = StringField()
    lang = StringField()
    public_metrics = EmbeddedDocumentField('TweetPublicMetrics')
    context_annotations = ListField(DictField())
    entities = DictField()
    attachments = DictField()  # make more specific?
    geo = DynamicField()
    referenced_tweets = ListField(EmbeddedDocumentField('TweetReference'))
    media = EmbeddedDocumentField('TwitterMedia')  # we store media directly in the tweet
    fetch_date = DateTimeField(default=datetime.utcnow)


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
