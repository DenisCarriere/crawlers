#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras


class Postgres(object):

    def __init__(self, name):
        self.name = name

        # Main Functions
        self.connect()

    def connect(self):
        """ Connects the the Postgres database"""

        self.conn = psycopg2.connect(database=self.name, user='postgres')
        self.c = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        self.status = {1: 'OK'}[self.conn.status]

    def sql(self, sql_statement):
        """ Executes the SQL Statement and returns the Dictionary """

        self.c.execute(sql_statement)
        return self.c.fetchall()

    def save(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

    def __repr__(self):
        return '<Postgres Database {0} [{1}]>'.format(self.name, self.status)

if __name__ == '__main__':
    db = Postgres('geocode')
    print db
