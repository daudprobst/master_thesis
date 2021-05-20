from mongoengine import connect


def connect_to_mongo(db_name: str = 'firestorms'):
    connect(db_name)
