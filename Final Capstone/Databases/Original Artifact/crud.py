from pymongo import MongoClient

class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """

    def __init__(self, user, password, host, port, db, collection):
        # Initialize connection to MongoDB with authentication
        self.client = MongoClient(host, port, username=user, password=password)
        self.db = self.client[db]
        self.collection = self.db[collection]
        print("Successfully connected to MongoDB")

    # Implement CRUD operations (create, read, update, delete) as before...
    def create(self, data):
        if data:
            try:
                self.collection.insert_one(data)
                return True
            except Exception as e:
                print(f"Error inserting document: {e}")
                return False
        else:
            raise ValueError("Nothing to save, because data parameter is empty")

    def read(self, query={}):
        try:
            cursor = self.collection.find(query)
            return list(cursor)
        except Exception as e:
            print(f"Error reading documents: {e}")
            return []

    def update(self, query, new_values):
        try:
            result = self.collection.update_many(query, {"$set": new_values})
            return result.modified_count
        except Exception as e:
            print(f"Error updating documents: {e}")
            return 0

    def delete(self, query):
        try:
            result = self.collection.delete_many(query)
            return result.deleted_count
        except Exception as e:
            print(f"Error deleting documents: {e}")
            return 0
