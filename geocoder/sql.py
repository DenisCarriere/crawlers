#!/usr/bin/python
# -*- coding: utf-8 -*-

def search(provider='Bing', city='ottawa'):
    return """
        SELECT location, ST_X(geom) as lng, ST_Y(geom) as lat
        FROM {city}
        WHERE NOT EXISTS (
            SELECT location
            FROM geocoder
            WHERE {city}.location = geocoder.location AND
            provider = '{provider}')
        ORDER BY random()
        """.format(city=city, provider=provider)

def exists(provider='Bing', location='<address>'):
    return """
        SELECT location FROM geocoder
        WHERE provider='{provider}' AND location='{location}'
        """.format(provider=provider, location=location)


def insert(fieldnames=['location', 'data', 'provider', 'distance', 'geom'], lat='', lng=''):
    len_fieldnames = len(fieldnames)

    if bool(lat and lng):
        geom = ", ST_GeomFromText('POINT({lng} {lat})', 4326)".format(lat=lat, lng=lng)
        len_fieldnames -= 1
    else:
        geom = str('')

    rows = ', '.join(fieldnames)
    values = ','.join(['%s'] * len_fieldnames)

    return """
        INSERT INTO geocoder ({rows})
        VALUES({values}{geom})
        """.format(rows=rows, values=values, geom=geom)

def distance(lat1, lng1, lat2, lng2):
    return """
        SELECT ST_Distance(
            ST_GeomFromText('POINT({0} {1})', 4326),
            ST_GeomFromText('POINT({2} {3})', 4326), true)
        AS distance
        """.format(lat1, lng1, lat2, lng2)