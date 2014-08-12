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

# Find Existing in Geocoder
existing = dict()
for item in db_geocoder.find({'provider':provider, 'city':city}):
    existing[item['location']] = ''
print 'Existing:', len(existing)

# Find Existing in City
search = list()
for item in db_city.find().skip(22000).limit(50000):
    if not item['location'] in existing:
        search.append(item)
print 'Remaining:', len(search)

# Scan Database
for item in search:
    location = item['location']
    x, y = item['geometry']['coordinates']
        
    # Geocode Address
    if provider == 'bing': 
        g = geocoder.bing(location)
    elif provider == 'osm':
        time.sleep(1)
        g = geocoder.osm(location)

    # Calculate Distance with Haversine formula
    distance = haversine([y, x], [g.lat, g.lng]) * 1000

    # Set Results
    results = g.json
    results['city'] = city
    results['distance'] = distance

    # Save in Mongo DB
    try:
        db_geocoder.insert(results)
    except:
        print 'Duplicate'

    try:
        print '[{0}] {1} ({2}) {3}'.format(g.status, provider, distance, location)
    except:
        pass