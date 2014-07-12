#!/usr/bin/python
# -*- coding: utf-8 -*-
import requests
import geocode
import grid
import json
import csv
from os import system
import sys
import time


class MLS(object):
    """
    Crawler API - MLS
    =================

    >> import crawler
    >> result = crawler.mls('Ottawa, ON')
    """

    def __init__(self, address, save_path='', box=''):
        self.address = address
        self.box = box
        self.inside = {}
        self.retry = {}
        self.save_path = save_path

        # Main Functions
        self.declare_blank()
        self.sys_argv()
        box = self.geocode()
        self.initialize(box)
        self.loop_main(box)
        self.loop_retry()

        if self.save_path:
            self.save(self.save_path)

    def sys_argv(self):
        """ If User defined Arguments in Python Terminal when running the script """

        # Define an Address from Terminal
        if len(sys.argv) >= 2:
            self.address = sys.argv[1]

        # Save File at the end
        if len(sys.argv) >= 3:
            self.save_path = sys.argv[2]

    def declare_blank(self):
        """ Declares empty variables that are used in this object """

        self.name = 'MLS Crawler'
        self.results = ''
        self.json = ''
        self.count = 0
        self.error_count = 0
        self.building_type = 0
        self.property_type = 300
        self.last_progress = -1

    def geocode(self):
        """ Will get Bounding Box from Google Geocode """

        if self.address:
            self.address = geocode.google(self.address)
            return self.address.box

        if self.box:
            return self.box

    def initialize(self, box):
        """ Returns all the info from the Crawler """

        # ISO 8601 Date Standards YYYY-MM-DD
        t = time.localtime()
        self.current_date = '{0}-{1}-{2}'.format(t.tm_year, t.tm_mon, t.tm_mday)

        # Residential
        self.property_type = 300
        self.request_mls(box)
        count_residential = self.count
        self.count = 0

        # Agricultural
        self.property_type = 302
        self.request_mls(box)
        count_agricultural = self.count
        self.count = 0

        # Land
        self.property_type = 303
        self.request_mls(box)
        count_land = self.count
        self.count_total = count_residential + count_agricultural + count_land
        self.count = 0

        print('MLS Crawler')
        print('===========')
        print('Location: {0}'.format(self.address.name))
        print('Box: {0}'.format(box))
        print('Save Path: {0}'.format(self.save_path))
        print('')
        print('MLS Listings')
        print('============')
        print('>> Residential: {0}'.format(count_residential))
        print('>> Agricultural: {0}'.format(count_agricultural))
        print('>> Land: {0}'.format(count_land))
        print('>> Total: {0}'.format(self.count_total))
        print('')

    def loop_main(self, box):

        self.building_type_lookup = {
            1: 'House',
            2: 'Duplex',
            3: 'Triplex',
            5: 'Residential Commercial Mix',
            6: 'Mobile Home',
            12: 'Special Purpose',
            14: 'Other',
            16: 'Row/Townhouse',
            17: 'Apartment',
            19: 'Fourplex',
            20: 'Garden Home',
            27: 'Manufactured Home/Mobile'
        }

        self.property_type_lookup = {
            300: 'Residential',
            301: 'Recreational',
            302: 'Agricultural',
            303: 'Land'
        }

        print('')
        print('CRAWLING Agricultural')
        print('=====================')
        self.property_type = 302
        self.building_type = ''
        self.request_mls(box=box)
        self.parse_results(box=box)

        print('')
        print('CRAWLING Land')
        print('=============')
        self.property_type = 303
        self.building_type = ''
        self.request_mls(box=box)
        self.parse_results(box=box)

        print('')
        print('CRAWLING Residential')
        print('====================')
        for building_type in self.building_type_lookup.keys():
            self.property_type = 300
            self.building_type = building_type
            self.request_mls(box=box)
            self.parse_results(box=box)

    def declare_xml(self, box):
        """ Declare XML with users settings """

        east, north = box[0]
        west, south = box[1]

        xml = "<ListingSearchMap>"
        # <ListingStartDate>24/03/2013</ListingStartDate>
        if self.building_type:
            xml += "<BuildingTypeIDs>{building_type}</BuildingTypeIDs>".format(building_type=self.building_type)
        xml += "<LatitudeMax>{north}</LatitudeMax>".format(north=north)
        xml += "<LatitudeMin>{south}</LatitudeMin>".format(south=south)
        xml += "<LongitudeMax>{east}</LongitudeMax>".format(east=east)
        xml += "<LongitudeMin>{west}</LongitudeMin>".format(west=west)
        xml += "<PriceMax>0</PriceMax>"
        xml += "<PriceMin>0</PriceMin>"
        xml += "<PropertyTypeID>{property_type}</PropertyTypeID>".format(property_type=self.property_type)
        xml += "<TransactionTypeID>2</TransactionTypeID>"
        xml += "<MinBath>0</MinBath>"
        xml += "<MaxBath>0</MaxBath>"
        xml += "<MinBed>0</MinBed>"
        xml += "<MaxBed>0</MaxBed>"
        xml += "<StoriesTotalMin>0</StoriesTotalMin>"
        xml += "<StoriesTotalMax>0</StoriesTotalMax>"
        xml += "</ListingSearchMap>"
        return xml

    def request_mls(self, box):
        """ Connects to MLS server and requests XML paramaters"""

        # Reset Main Values
        self.count = 0
        self.results = ''
        self.json = ''

        # Request URL
        xml = self.declare_xml(box=box)
        MLS_URL = 'http://www.realtor.ca/handlers/MapSearchHandler.ashx'
        url = '{0}?xml={1}'.format(MLS_URL, xml)
        try:
            r = requests.get(url, timeout=5.0)
            self.status_code = r.status_code
        except:
            self.status_code = 404

        # Connection Successful
        if self.status_code == 200:
            # Get Results from JSON object
            try:
                self.json = json.loads(r.content.replace("\\'", "").replace('\\"', '').replace('\\', ''))
            except ValueError:
                print('Corrupt JSON')
                print(url)
                self.retry[self.error_count] = [box, self.building_type, self.property_type]
                self.error_count += 1

            if self.json:
                self.results = self.json.get('MapSearchResults')
                self.count = self.json.get('NumberSearchResults')

        else:
            print('URL Error', self.status_code)
            print(url)
            self.retry[self.error_count] = [box, self.building_type, self.property_type]
            self.error_count += 1

    def parse_results(self, box):
        """ Checks if results are good """

        if self.results:
            self.store_values()
            self.print_progress()

        # If results exceed Maximum requests Split Area
        elif (self.count >= 500):
            square = grid.square(box, column=2, row=2)
            for point in square:
                self.request_mls(box=point.box)
                self.parse_results(box=point.box)

    def join_list(self, item, value):
        """ Join a list to a string """

        lists = item.get(value)
        if lists:
            return ', '.join(lists)
        else:
            return ''

    def store_values(self):
        """ Stores MLS values inside self """

        # Loop inside each listing within the results
        for item in self.results:
            mls = item.get('MLS')
            x, y = float(item.get('Longitude')), float(item.get('Latitude'))

            if mls in self.inside:
                print(mls, 'Duplicate', self.building_type, self.property_type)

            if (mls not in ['ERROR']) and ([x, y] != [0.0, 0.0]):
                self.inside[mls] = {
                    'x': x,
                    'y': y,
                    'WKT': 'POINT({x} {y})'.format(x=x, y=y),
                    'mls': mls,
                    'city': item.get('City'),
                    'date': self.current_date,
                    'realtor': self.join_list(item, 'OrganizationName'),
                    'building_type': self.building_type_lookup.get(self.building_type),
                    'property_type': self.property_type_lookup.get(int(item.get('PropertyTypeID'))),
                    'land_size': item.get('LandSize'),
                    'farm_type': self.join_list(item, 'FarmType'),
                    'parking_type': self.join_list(item, 'ParkingType'),
                    'price': item.get('Price').replace('$', '').replace(',', ''),
                    'address': item.get('Address'),
                    'basement_bedroom': item.get('BedroomsBelowGround'),
                    'above_bedroom': item.get('BedroomsAboveGround'),
                    'bedrooms': item.get('Bedrooms'),
                    'bathrooms': item.get('Bathrooms')
                }
            else:
                pass
                #print('{0} MLS Feature does not contain any spatial reference.'.format(mls))

    def print_progress(self):
        """ Prints the remaining download percent """

        current_progress = int(len(self.inside) / float(self.count_total) * 100)
        if current_progress is not self.last_progress:
            print('{0}%'.format(current_progress))
        self.last_progress = current_progress

    def loop_retry(self):
        """ Restarts all the corrupted JSON/URL requests """

        retry_count = 0

        if self.retry:
            print('')
            print('FIXING Corrupted')
            print('================')

        while len(self.retry) != 0 and retry_count < 10:
            for key, value in self.retry.items():
                print(key, value)
                box, self.building_type, self.property_type = value
                self.request_mls(box=box)
                del self.retry[key]
                retry_count += 1
        print('')
        print('Done! :)')

    def save(self, path):
        """ Saves to the local file """

        f = open('MLS/{0}_{1}'.format(self.current_date, path), 'wb')
        f.write(u'\ufeff'.encode('utf8'))

        # Get Fieldnames for CSV Dictionary, getting first row of Inside
        fieldnames = [
            'mls',
            'x',
            'y',
            'WKT',
            'price',
            'date',
            'property_type',
            'building_type',
            'address',
            'city',
            'bedrooms',
            'bathrooms',
            'basement_bedroom',
            'above_bedroom',
            'land_size',
            'farm_type',
            'parking_type',
            'realtor']

        w = csv.DictWriter(f, fieldnames=fieldnames, dialect='excel-tab')
        w.writeheader()

        for row in self.inside.values():
            encoded_row = {}
            for key, value in row.items():
                if value:
                    encoded_row[key] = unicode(value).encode('utf8')
                else:
                    encoded_row[key] = ''
            w.writerow(encoded_row)
        f.close()

    def __repr__(self):
        return '<{name} [{count}]>'.format(
            name=self.name,
            count=len(self.inside))

    def __iter__(self):
        return iter(self.inside.values())


if __name__ == '__main__':
    crawler = MLS('Canada', save_path='Canada.csv')
    print(crawler)
