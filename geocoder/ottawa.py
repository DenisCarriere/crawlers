#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras
import geocoder
import json


host = 'kingston.cbn8rngmikzu.us-west-2.rds.amazonaws.com'
conn = psycopg2.connect(host=host, port=5432, dbname='mydb', user='addxy', password='Denis44C')
c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

sql_search = """
SELECT gid, location, ST_X(geom) as lng, ST_Y(geom) as lat
FROM ottawa
WHERE NOT EXISTS (
    SELECT location
    FROM geocoder
    WHERE ottawa.location = geocoder.location AND
    geocoder.data->>'provider' = 'Google')
ORDER BY random()
"""

sql_exists = """
SELECT location FROM geocoder
WHERE geocoder.data->>'provider'=%s AND location=%s
"""

sql_insert = """
INSERT INTO geocoder (location, data, geom)
VALUES(%s,%s, ST_GeomFromText('POINT({lng} {lat})', 4326))
"""


# Loop inside all the data from Ottawa
c.execute(sql_search)
for row in c.fetchall():
    location = row['location']
    lng = row['lng']
    lat = row['lat']
    sql_insert = sql_insert.format(lat=lat, lng=lng)

    # Provider
    if location:
        c.execute(sql_exists, ('Bing', location))
        if not c.fetchone():
            g = geocoder.bing(location.strip())
            if g.ok:
                c.execute(sql_insert,(location, json.dumps(g.json)))
                print location
                conn.commit()
