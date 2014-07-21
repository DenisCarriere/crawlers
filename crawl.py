#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras
import geocoder
import json

# Connect PostGIS Server
conn = psycopg2.connect(host='localhost', port=5432, dbname='postgres', user='postgres', password='Denis44C')
c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Main Functions
def sql_calculate_distance(c, x1, y1, x2, y2):
    sql = """SELECT ST_Distance(
        ST_GeomFromText('POINT({0} {1})', 4326),
        ST_GeomFromText('POINT({2} {3})', 4326), true)
        AS distance""".format(x1, y1, x2, y2)
    c.execute(sql)
    return c.fetchone()['distance']

def sql_search_remaining(c, provider, city):
    sql = """SELECT gid, location, ST_X(geom) as x, ST_Y(geom) as y from {city}
        WHERE NOT EXISTS (
        SELECT location
        FROM geocoder
        WHERE geocoder.location = {city}.location 
        AND provider = %s
        AND city = %s)""".format(city=city)

    c.execute(sql, (provider, city))
    return c.fetchall()

def sql_check_exists(c, location, provider):
    sql = "SELECT True FROM geocoder WHERE location=%s AND data->>'provider'=%s"
    c.execute(sql, (location, provider))
    if c.fetchone():
        return True
    else:
        return False

def select_provider(location, provider):
    if provider == 'Bing':
        return geocoder.bing(location)
    elif provider == 'Google':
        return geocoder.google(location)
    elif provider == 'OSM':
        return geocoder.osm(location)
    else:
        raise('Invalid Provider, Select from (Bing, Google, OSM)')

def sql_insert_row(c, location, address, data, distance, city, provider, wkt, status, x, y, lat, lng):
    geom = "ST_GeomFromText('POINT({x} {y})', 4326)".format(x=x, y=y)
    sql = """INSERT INTO geocoder (
        location,
        address,
        data, 
        distance, 
        city, 
        provider,
        wkt,
        status,
        lat,
        lng,
        geom) 
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, {geom})""".format(geom=geom)
    return c.execute(sql, (location, address, data, distance, city, provider, wkt, status, lat, lng))


if __name__ == '__main__':
    import sys


    # Main Variables
    provider = 'Bing'
    if sys.argv > 2:
        city = sys.argv[-1].lower()
    else:
        city = 'waterloo'

    # Main Program
    for item in sql_search_remaining(c, provider, city):

        # Retrieve Variables for City Database
        gid = item['gid']
        location = item['location']
        x = item['x']
        y = item['y']

        # Check Database if location already exists
        if not sql_check_exists(c, location, provider):

            # Select Provider
            g = select_provider(location, provider)

            # Insert Geocoded Data into Database
            data = json.dumps(g.json)
            distance = sql_calculate_distance(c, x1=g.lng, y1=g.lat, x2=x, y2=y)

            # INSERT Function
            sql_insert_row(c, 
                location=location,
                address=g.address,
                data=data, 
                distance=distance,
                city=city, 
                provider=provider,
                wkt=g.wkt,
                status=g.status,
                x=x, y=y, 
                lat=g.lat, lng=g.lng)
            conn.commit()

            print distance, '-', g
        else:
            print 'SKIPPED!',location
