#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  2 19:30:47 2024

@author: tylerdickerso_snhu
"""

from pymongo import MongoClient, ASCENDING, DESCENDING
from bson.objectid import ObjectId
from pymongo.encryption_options import AutoEncryptionOpts

class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """

    def __init__(self, USER='aacuser', PASS='red9', HOST='nv-desktop-services.apporto.com', PORT=31399, DB='AAC', COL='animals'):
        # Initialize Connection
        self.client = MongoClient(f'mongodb://{USER}:{PASS}@{HOST}:{PORT}')
        self.database = self.client[DB]
        self.collection = self.database[COL]
        print("Successfully connected")

        # Create indexes for optimized queries
        self.collection.create_index([('field1', ASCENDING), ('field2', DESCENDING)])
        
        # Configure encryption options (this is a placeholder, actual configuration may vary)
        self.encryption_opts = AutoEncryptionOpts(
            kms_providers={'local': {'key': b'\x00' * 96}},
            key_vault_namespace='admin.datakeys'
        )

    def create(self, data):
        if data:
            self.collection.insert_one(data, encryption=self.encryption_opts)  # Insert data with encryption
            return True
        else:
            raise Exception("Nothing to save, because data parameter is empty")

    def read(self, criteria=None):
        if criteria is None:
            return list(self.collection.find())  # Return all documents if no criteria provided
        else:
            return list(self.collection.find(criteria))  # Return documents matching the criteria

    def update(self, query, new_values):
        if query and new_values:
            result = self.collection.update_many(query, {"$set": new_values})
            return result.modified_count
        else:
            raise Exception("Query and new values must be provided")

    def delete(self, query):
        if query:
            result = self.collection.delete_many(query)
            return result.deleted_count
        else:
            raise Exception("Query must be provided")

    def advanced_query(self, field1_value):
        pipeline = [
            {'$match': {'field1': field1_value}},
            {'$group': {'_id': '$field2', 'count': {'$sum': 1}}}
        ]
        return list(self.collection.aggregate(pipeline))

    def deploy_kubernetes(self):
        # Pseudocode: Actual deployment steps would be defined in Kubernetes YAML files
        # Deploy the application and MongoDB instance on Kubernetes
        pass

if __name__ == "__main__":
    shelter = AnimalShelter()
    # Example operations
    shelter.create({"field1": "value1", "field2": "value2"})
    print(shelter.read({"field1": "value1"}))
    shelter.update({"field1": "value1"}, {"field2": "new_value"})
    shelter.delete({"field1": "value1"})
    print(shelter.advanced_query("value1"))
