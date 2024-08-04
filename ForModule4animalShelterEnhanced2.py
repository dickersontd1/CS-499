#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  2 19:30:47 2024

@author: tylerdickerso_snhu
"""

from pymongo import MongoClient, ASCENDING, DESCENDING
from bson.objectid import ObjectId
from pymongo.encryption import ClientEncryption
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
import base64
import os

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
        self.collection.create_index([('field3', ASCENDING)])  # Adding an additional index for optimization

        # Generate an encryption key
        key = self.generate_encryption_key()
        kms_providers = {'local': {'key': key}}
        
        # Configure encryption options
        self.encryption = ClientEncryption(
            kms_providers,
            'admin.datakeys',
            self.client,
            default_backend()
        )

    def generate_encryption_key(self):
        # Generate a secure key for local encryption
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b'\x00' * 16,
            iterations=100000,
            backend=default_backend()
        )
        return base64.urlsafe_b64encode(kdf.derive(b'my_secret_password'))

    def create(self, data):
        if data:
            try:
                encrypted_data = {k: self.encryption.encrypt(v, 'AEAD_AES_256_CBC_HMAC_SHA_512-Random') for k, v in data.items()}
                self.collection.insert_one(encrypted_data)
                return True
            except Exception as e:
                raise Exception(f"An error occurred while saving data: {e}")
        else:
            raise Exception("Nothing to save, because data parameter is empty")

    def read(self, criteria=None):
        try:
            documents = self.collection.find(criteria) if criteria else self.collection.find()
            return [{k: self.encryption.decrypt(v) if isinstance(v, bytes) else v for k, v in doc.items()} for doc in documents]
        except Exception as e:
            raise Exception(f"An error occurred while reading data: {e}")

    def update(self, query, new_values):
        if query and new_values:
            try:
                encrypted_values = {k: self.encryption.encrypt(v, 'AEAD_AES_256_CBC_HMAC_SHA_512-Random') for k, v in new_values.items()}
                result = self.collection.update_many(query, {"$set": encrypted_values})
                return result.modified_count
            except Exception as e:
                raise Exception(f"An error occurred while updating data: {e}")
        else:
            raise Exception("Query and new values must be provided")

    def delete(self, query):
        if query:
            try:
                result = self.collection.delete_many(query)
                return result.deleted_count
            except Exception as e:
                raise Exception(f"An error occurred while deleting data: {e}")
        else:
            raise Exception("Query must be provided")

    def advanced_query(self, field1_value):
        try:
            pipeline = [
                {'$match': {'field1': field1_value}},
                {'$group': {'_id': '$field2', 'count': {'$sum': 1}}},
                {'$sort': {'count': DESCENDING}}  # Adding a sort stage to the pipeline for better analysis
            ]
            return list(self.collection.aggregate(pipeline))
        except Exception as e:
            raise Exception(f"An error occurred while performing advanced query: {e}")

    def bulk_insert(self, data_list):
        """ Efficiently insert multiple documents into the collection """
        if data_list:
            try:
                encrypted_data_list = [{k: self.encryption.encrypt(v, 'AEAD_AES_256_CBC_HMAC_SHA_512-Random') for k, v in data.items()} for data in data_list]
                self.collection.insert_many(encrypted_data_list)
                return True
            except Exception as e:
                raise Exception(f"An error occurred while bulk inserting data: {e}")
        else:
            raise Exception("Data list is empty")

    def deploy_kubernetes(self):
        # Pseudocode: Actual deployment steps would be defined in Kubernetes YAML files
        # Deploy the application and MongoDB instance on Kubernetes
        pass

if __name__ == "__main__":
    shelter = AnimalShelter()
    # Example operations
    shelter.create({"field1": "value1", "field2": "value2", "field3": "value3"})
    print(shelter.read({"field1": "value1"}))
    shelter.update({"field1": "value1"}, {"field2": "new_value"})
    shelter.delete({"field1": "value1"})
    print(shelter.advanced_query("value1"))
    shelter.bulk_insert([
        {"field1": "value1", "field2": "value2", "field3": "value3"},
        {"field1": "value4", "field2": "value5", "field3": "value6"}
    ])
