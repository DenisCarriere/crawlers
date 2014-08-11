import geocoder
import time
from pymongo import MongoClient
from haversine import haversine

# Program Variables
provider = 'Bing'
city = 'ottawa'

# Connec to MongoDB
MONGO_URL = os.environ.get('MONGOHQ_URL')
client = MongoClient(MONGO_URL)
db = client['geocoder']
db_ottawa = db[city]
db_geocoder = db['geocoder']

# Find Existing
existing = dict()
for item in db_geocoder.find({'provider':provider, 'city':city}):
    existing[item['location']] = ''
print 'Existing:', len(existing)

search = list()
for item in db_ottawa.find():
    if not item['location'] in existing:
        search.append(item)
print 'Remaining:', len(search)

# Scan Database
for item in search:
    location = item['location']
    x, y = item['loc']['coordinates']
        
    # Geocode Address
    if provider == 'Bing': 
        g = geocoder.bing(location)
    elif provider == 'OSM':
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
        print '[{0}] {1} ({2}) {3}'.format(g.status, provider, distance, location)
    except:
        print 'Duplicate -', location