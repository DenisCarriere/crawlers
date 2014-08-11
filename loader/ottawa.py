import geocoder
import csv
import os
from pymongo import MongoClient
from osgeo import gdal

MONGO_URL = os.environ.get('MONGOHQ_URL')
client = MongoClient(MONGO_URL)
db = client['geocoder']
db_city = db['ottawa']

# Retrieve existing addresses from MongoDB
existing = dict()
for item in db_city.find():
	existing[item['location']] = ''
print 'Existing:',len(existing)

# Open File Name
with open('data/ottawa_address_WGS84.csv', 'rb') as f:
	reader = csv.DictReader(f)

	for item in reader:
		# Load Attributes
		x = float(item['X'])
		y = float(item['Y'])
		street_number = int(item.get('addrNumber', ''))
		street_name = item.get('RoadName', '').title()
		street_suffix = item.get('RdType', '').title()
		street_direction = item.get('RdDir')
		district = ''

		# Formatting
		route = '{0} {1} {2}'.format(street_number, street_name, street_suffix).strip()
		route = '{0} {1}'.format(route, street_direction).strip()
		city = 'Ottawa'
		province = 'Ontario'
		if district:
			location = '{0}, {1}, {2}, {3}'.format(route, district, city, province).strip()
		else:
			location = '{0}, {1}, {2}'.format(route, city, province).strip()

		# Save in MongoDB
		row = dict()

		# GeoJSON format for Geometry
		row['geometry'] = { 'type': 'Point', 'coordinates': [x,y] }
		row['location'] = location
		
		if street_number:
			row['street_number'] = street_number
		if street_name:
			row['street_name'] = street_name
		if street_suffix:
			row['street_suffix'] = street_suffix
		if street_direction:
			row['street_direction'] = street_direction
		if route:
			row['route'] = route
		if district:
			row['district'] = district
		row['city'] = city
		row['province'] = province

		if not location in existing:
			try:
				db_city.insert(row)
				print 'Insert:', location
			except:
				print 'Duplicate:', location