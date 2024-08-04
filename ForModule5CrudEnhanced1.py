from pymongo import MongoClient, ASCENDING
from pymongo.errors import OperationFailure
import logging
from pydantic import BaseModel, ValidationError

# Define a data model for validation
class AnimalData(BaseModel):
    name: str
    species: str
    age: int
    breed: str

class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """

    def __init__(self, user, password, host, port, db, collection):
        # Initialize connection to MongoDB with authentication
        self.client = MongoClient(host, port, username=user, password=password)
        self.db = self.client[db]
        self.collection = self.db[collection]
        print("Successfully connected to MongoDB")

        # Create an index on the 'name' field to optimize queries
        self.collection.create_index([('name', ASCENDING)])
        logging.basicConfig(filename='animal_shelter.log', level=logging.INFO)

    def create(self, data):
        try:
            # Validate the data against the schema
            valid_data = AnimalData(**data)
            self.collection.insert_one(valid_data.dict())
            logging.info("Document inserted successfully")
            return True
        except ValidationError as ve:
            logging.error(f"Validation error: {ve}")
            return False
        except Exception as e:
            logging.error(f"Error inserting document: {e}")
            return False

    def read(self, query={}):
        try:
            cursor = self.collection.find(query)
            documents = list(cursor)
            logging.info(f"Read {len(documents)} documents")
            return documents
        except Exception as e:
            logging.error(f"Error reading documents: {e}")
            return []

    def update(self, query, new_values):
        try:
            result = self.collection.update_many(query, {"$set": new_values})
            logging.info(f"Updated {result.modified_count} documents")
            return result.modified_count
        except Exception as e:
            logging.error(f"Error updating documents: {e}")
            return 0

    def delete(self, query):
        try:
            result = self.collection.delete_many(query)
            logging.info(f"Deleted {result.deleted_count} documents")
            return result.deleted_count
        except Exception as e:
            logging.error(f"Error deleting documents: {e}")
            return 0
