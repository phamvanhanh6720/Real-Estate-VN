import os
import pymongo
from configparser import ConfigParser

import boto3

config = ConfigParser()
config.read(os.path.join('config.ini'))


def fetch_data_in_batches(database_name, collection_name, batch_size, query=None):
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
        if query is None:
            documents = collection.find().skip(start).limit(batch_size)
        else:
            documents = collection.find(query).skip(start).limit(batch_size)

        # Process and yield the documents
        batch = []
        for document in documents:
            document['_id'] = str(document['_id'])
            batch.append(document)

        if not len(batch):
            break

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


def upload_folder_to_s3(bucket_name, folder_path, prefix):
    # Create an AWS session
    session = boto3.Session(
        aws_access_key_id=config.get('AWS', 'AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=config.get('AWS', 'AWS_SECRET_ACCESS_KEY'),
        region_name=config.get('AWS', 'AWS_DEFAULT_REGION')
    )
    s3 = session.client('s3')

    # Iterate over the files in the folder
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            # Construct the full local path of the file
            local_path = os.path.join(root, file)

            # Construct the S3 key by removing the folder path prefix
            s3_key = os.path.join(prefix, file)

            # Upload the file to S3
            s3.upload_file(local_path, bucket_name, s3_key)
