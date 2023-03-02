from pymongo import MongoClient
import json
import itertools

def get_database():
    CONNECTION_STRING = "mongodb://user:pass@localhost:27017/?authMechanism=SCRAM-SHA-256&authSource=admin"
    client = MongoClient(CONNECTION_STRING)
    return client["shad_homework"]

def get_collection(collection):
    return get_database()[collection]

def add_object(collection, object_to_add):
    collection = get_collection(collection)
    collection.insert_one(object_to_add)

def update_object(collection, matcher, updater):
    collection = get_collection(collection)
    collection.update_one(matcher, updater)

def execute_pipeline(collection, pipeline, save_to, limit=None):
    collection = get_collection(collection)
    results = list(itertools.islice(collection.aggregate(pipeline), limit))
    with open(save_to, 'w') as fw:
        json.dump(results, fw, default=str, indent=2)
