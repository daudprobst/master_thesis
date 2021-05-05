from mongoengine import connect


def connect_to_mongo(db_name: str = 'firestorms'):
    print(f'Connecting to mongo_db with name {db_name}...')
    connect('firestorms')
    print(f'Connected!')