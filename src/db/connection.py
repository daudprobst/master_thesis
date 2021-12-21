from mongoengine import connect


def connect_to_mongo(db_name: str = "firestorms") -> None:
    """Open a connection to the specified mongo database

    :param db_name: database to which the connection should be opened, defaults to "firestorms"
    """
    connect(db_name, serverSelectionTimeoutMS=3000)
