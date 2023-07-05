import os
import pymongo
from configparser import ConfigParser


config = ConfigParser()
config.read(os.path.join('config.ini'))


def fetch_data_in_batches(database_name, collection_name, batch_size):
    # Connect to the MongoDB server
    client = pymongo.MongoClient(
        host=config.get('DB', 'HOST'),
        port=int(config.get('DB', 'PORT')),
        username=config.get('DB', 'USERNAME'),
        password=config.get('DB', 'PASSWORD'),
        authSource='admin',
        authMechanism='SCRAM-SHA-1'
    )

    # Access the database and collection
    db = client[database_name]
    collection = db[collection_name]

    # Define the starting point
    start = 0

    # Fetch data in batches
    while True:
        # Retrieve the batch of documents
        documents = collection.find().skip(start).limit(batch_size)

        # Process and yield the documents
        batch = []
        for document in documents:
            document['_id'] = str(document['_id'])
            batch.append(document)

        yield batch

        # Check if there are more documents
        count = collection.count_documents({})
        if count <= start + batch_size:
            # No more documents, exit the loop
            break

        # Increment the starting point for the next batch
        start += batch_size

    # Close the MongoDB connection
    client.close()
