from pymongo import MongoClient
import os


# Program Variables
provider = 'bing'
city = 'ottawa'

# Connec to MongoDB
MONGO_URL = os.environ.get('MONGOHQ_URL')
client = MongoClient(MONGO_URL)
db = client['geocoder']
db_city = db[city]
db_geocoder = db[provider]


# Finds
ok = db_geocoder.find({'status':{'$ne':'OK'}})
distance = db_geocoder.find({'distance':{'$gt':5000}, 'status':{'$ne':'ERROR - No results found'}})

print ok.count()
print distance.count()

"""
for item in distance:
	print item.get('location'), '                ------', item.get('address')
"""