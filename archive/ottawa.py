#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras
import geocoder
import json
import sql

host = 'kingston.cbn8rngmikzu.us-west-2.rds.amazonaws.com'
conn = psycopg2.connect(host=host, port=5432, dbname='mydb', user='addxy', password='Denis44C')
c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
provider = 'Bing'
city = 'ottawa'

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
