#!/usr/bin/python

# 2852036

import sys
import re
import datetime
import requests
import csv
import simplejson
import codecs
import cStringIO
from requests.auth import HTTPBasicAuth


if len(sys.argv) != 4:
    sys.stderr.write("Usage: %s <organization> <flow> <token>\n" % sys.argv[0])
    sys.exit(1)

(organization, flow, token) = sys.argv[1:]

class Flowdock():

    API = "https://api.flowdock.com/v1" #"flows/%s/%s"
    
    def __init__(self, organization, flow, token):
        self.organization = organization
        self.flow = flow
        self.token = token

    def get(self, url, **kwargs):
        r = requests.get(url,
                         auth=HTTPBasicAuth(token, ''),
                         **kwargs)
        return simplejson.loads(r.content)
        

    def fetch_user(self, id):
        """
        Fetches the details of a Flowdock user with 'id'. Returns a dict.
        """
        url = self.API + "/users/%s" % id
        return self.get(url)


    def fetch_messages(self, params):
        """
        Fetches a set of messages from the flow. 'params' used for the
        filtering GET parameters. Returns a list of dicts.
        """
        url = self.API + "/flows/%s/%s/messages" % \
            (self.organization, self.flow)
        return self.get(url, params=params)

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """
    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([unicode(s).encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

users = {}
files = {}
data = []

fd = Flowdock(organization, flow, token)

since_id = 0

params = {'tags': 'done', 'event': 'message', 'limit': '100', 'sort': 'asc'}

while True:
    params['since_id'] = since_id
    dataset = fd.fetch_messages(params)
    if dataset:
        data.extend(dataset)
        since_id = dataset[-1]['id']
    else:
        break

for msg in data:
    user_id = msg['user']
    if user_id not in users:
        users[user_id] = fd.fetch_user(user_id)
    username = users[user_id]['nick']

    wp = re.compile(r'\s+')
    content = wp.split(msg['content'])
    # assert content[0] == '#done'

    hours = content[1]
    pct = content[2]
    description = ' '.join(content[3:])
    sent = datetime.datetime.utcfromtimestamp(msg['sent'] / 1000)
    if re.match(r'^\d{4}-\d{2}-\d{2}$', content[3]):
        date = content[3]
        description = ' '.join(content[4:])
    else:
        date = sent.strftime('%Y-%m-%d')

    if username not in files:
        fh = open('tmp/%s.csv' % username, 'wb')
        files[username] = UnicodeWriter(fh)

    row = [hours, pct, date, description,
           sent.strftime('%Y-%m-%d %H:%M:%S'), msg['id']]
    files[username].writerow(row)

    print "%s %s %s %s" % (date, username, hours, pct)


    
    
    
