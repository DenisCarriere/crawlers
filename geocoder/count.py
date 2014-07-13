#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras

host = 'kingston.cbn8rngmikzu.us-west-2.rds.amazonaws.com'
conn = psycopg2.connect(host=host, port=5432, dbname='mydb', user='addxy', password='Denis44C')
c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

c.execute('SELECT count(1) from geocoder')
print c.fetchone()['count'] - 134622
