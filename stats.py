import geocoder
from pymongo import MongoClient
from haversine import haversine
import os
import time


# Program Variables
provider = 'osm'
city = 'ottawa'

# Connec to MongoDB
MONGO_URL = os.environ.get('MONGOHQ_URL')
client = MongoClient(MONGO_URL)
db = client['geocoder']
db_city = db[city]
db_geocoder = db[provider]
