#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras
import geocoder
import json


host = 'localhost'
conn = psycopg2.connect(host=host, port=5432, dbname='postgres', user='postgres', password='Denis44C')
c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
provider = 'Bing'
city = 'kingston'

def get_keys(items, key):
    container = {}
    for item in items:
        container[item[key]] = ''
    return container

# Existing Dataset
def get_existing(provider):
    c.execute("SELECT location FROM geocoder WHERE data->>'provider'=%s ORDER BY random()", (provider,))
    return get_keys(c.fetchall(), 'location')

existing = get_existing(provider)
print len(existing)
exit()

# Loop inside Dataset
count = 1
c.execute('SELECT location FROM {0}'.format(city))
for item in c.fetchall():
    location = item['location']

    # Check if Exists
    if not location in existing:
        count += 1

        # Select Provider
        if provider == 'Bing':
            g = geocoder.bing(location)
        elif provider == 'Google':
            g = geocoder.google(location)

        # Insert Geocoded Data into Database
        if g.ok:
            sql = "INSERT INTO geocoder (location, data, geom) VALUES (%s,%s, ST_GeomFromText('POINT({lng} {lat})', 4326))"
            sql = sql.format(lng=g.lng, lat=g.lat)
            c.execute(sql, (location, json.dumps(g.json)))
            print g
            conn.commit()

    # Restart certain values every 50 times
    if count % 50 == 0:
        existing = get_existing(provider)


"""
# Loop inside all the data from Ottawa
c.execute(sql.search(provider, city))

for row in c.fetchall():
    location = row['location']
    lng = row['lng']
    lat = row['lat']

    # Provider
    if bool(location and lng and lat):
        # Remove white spaces in location name
        location = location.strip()

        # Check if location already exists in Geocoder DB
        c.execute(sql.exists(),
            (provider, location))
        if not c.fetchone():
            # Select your provider for geocoding
            if provider == 'Bing':
                g = geocoder.bing(location)
            elif provider == 'Google':
                g = geocoder.google(location)

            # Geocode must be correct
            if g.ok:
                # Calculate Distance
                c.execute(sql.distance(lat, lng, g.lat, g.lng))
                distance = c.fetchone()['distance']

                # Insert Into Rows
                fieldnames = ['location', 'data', 'provider', 'distance', 'geom']
                c.execute(sql.insert(fieldnames, lat, lng),
                    (location, json.dumps(g.json), provider, distance))

                # Print Statement
                if distance > 500:
                    print distance, '-', location
                conn.commit()

"""