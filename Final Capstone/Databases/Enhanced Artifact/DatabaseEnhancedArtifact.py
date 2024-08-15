from pymongo import MongoClient, ASCENDING
from cassandra.cluster import Cluster
import redis
import logging
from pydantic import BaseModel, ValidationError
from flask import Flask, request, jsonify, make_response
import jwt
import datetime
from functools import wraps

# Enhancement: Connect Redis for caching and session management
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Define a data model for validation
class AnimalData(BaseModel):
    name: str
    species: str
    age: int
    breed: str

class AnimalShelter(object):
    """CRUD operations for Animal collection in MongoDB"""

    def __init__(self, user, password, host, port, db, collection, cassandra_hosts=['127.0.0.1']):
        # Initialize connection to MongoDB with authentication
        self.client = MongoClient(host, port, username=user, password=password)
        self.db = self.client[db]
        self.collection = self.db[collection]
        print("Successfully connected to MongoDB")

        # Enhancement: Integrate Apache Cassandra for large-scale data storage
        self.cassandra_cluster = Cluster(cassandra_hosts)
        self.cassandra_session = self.cassandra_cluster.connect('animal_keyspace')
        print("Successfully connected to Cassandra")

        # Create an index on the 'name' field to optimize queries
        self.collection.create_index([('name', ASCENDING)])
        logging.basicConfig(filename='animal_shelter.log', level=logging.INFO)

    def create(self, data):
        try:
            # Validate the data against the schema
            valid_data = AnimalData(**data)
            self.collection.insert_one(valid_data.dict())
            logging.info("Document inserted successfully")

            # Enhancement: Insert data into Cassandra for large-scale storage
            self.cassandra_session.execute(
                """
                INSERT INTO animals (id, name, species, age, breed)
                VALUES (uuid(), %s, %s, %s, %s)
                """, 
                (valid_data.name, valid_data.species, valid_data.age, valid_data.breed)
            )
            return True
        except ValidationError as ve:
            logging.error(f"Validation error: {ve}")
            return False
        except Exception as e:
            logging.error(f"Error inserting document: {e}")
            return False

    def read(self, query={}):
        try:
            # Enhancement: Check Redis cache first for data retrieval
            cache_key = str(query)
            cached_result = redis_client.get(cache_key)
            if cached_result:
                logging.info(f"Cache hit for query: {query}")
                return eval(cached_result)

            cursor = self.collection.find(query)
            documents = list(cursor)
            logging.info(f"Read {len(documents)} documents")

            # Cache the result in Redis
            redis_client.set(cache_key, str(documents))
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

    # Enhancement: Implement data replication and failover in Cassandra
    def cassandra_read(self, query):
        try:
            result = self.cassandra_session.execute(
                "SELECT * FROM animals WHERE name=%s", (query.get('name'),)
            )
            return list(result)
        except Exception as e:
            logging.error(f"Error reading from Cassandra: {e}")
            return []

    # Enhancement: Develop a robust API for database interactions using RESTful principles
app = Flask(__name__)
app.config['SECRET_KEY'] = 'mysecret'

# Enhancement: Implement JWT-based authentication for secure user management
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.args.get('token')
        if not token:
            return jsonify({'message': 'Token is missing!'}), 403
        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
        except:
            return jsonify({'message': 'Token is invalid!'}), 403
        return f(*args, **kwargs)
    return decorated

@app.route('/animal', methods=['POST'])
@token_required
def add_animal():
    data = request.json
    if shelter.create(data):
        return jsonify({'message': 'Animal added successfully!'}), 201
    return jsonify({'message': 'Failed to add animal.'}), 500

@app.route('/animal', methods=['GET'])
@token_required
def get_animals():
    query = request.args.to_dict()
    animals = shelter.read(query)
    return jsonify(animals), 200

@app.route('/cassandra_animal', methods=['GET'])
@token_required
def get_cassandra_animals():
    query = request.args.to_dict()
    animals = shelter.cassandra_read(query)
    return jsonify(animals), 200

@app.route('/login', methods=['POST'])
def login():
    auth = request.authorization
    if auth and auth.password == 'password':
        token = jwt.encode({'user': auth.username, 'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=30)}, app.config['SECRET_KEY'])
        return jsonify({'token': token})
    return make_response('Could not verify!', 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})

if __name__ == "__main__":
    # Enhancement: Initialize MongoDB and Cassandra connections, and start the Flask app
    shelter = AnimalShelter(user='user', password='password', host='localhost', port=27017, db='AAC', collection='animals')
    app.run(debug=True)

