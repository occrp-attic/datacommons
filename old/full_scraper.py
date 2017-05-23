import re
import os
import json
import logging
import requests
import dataset
# from pprint import pprint
from normality import slugify
from datetime import datetime
from itertools import count
import requests.packages.urllib3
from sqlalchemy.types import BigInteger

DATABASE_URI = os.environ.get('DATABASE_URI', 'sqlite:///:memory:')
assert DATABASE_URI is not None

DATA_PATH = os.environ.get('DATA_PATH', 'data')
assert DATA_PATH is not None

log = logging.getLogger(__name__)
database = dataset.connect(DATABASE_URI)

requests.packages.urllib3.disable_warnings()
logging.basicConfig(level=logging.INFO)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('alembic').setLevel(logging.WARNING)

# comment out countries here to disable scraping the
# respective countries.
SITES = {
    #'BW': 'http://portals.flexicadastre.com/botswana/',
    #'UG': 'http://portals.flexicadastre.com/uganda/',
    #'NA': 'http://portals.flexicadastre.com/Namibia/',
    #'MZ': 'http://portals.flexicadastre.com/mozambique/en/',
    'LR': 'http://portals.flexicadastre.com/liberia/',
    #'KE': 'http://map.miningcadastre.go.ke/map',
    #'RW': 'http://portals.flexicadastre.com/rwanda/',
    #'TZ': 'http://portal.mem.go.tz/map/',
    #'ZM': 'http://portals.flexicadastre.com/zambia/',
    #'CD': 'http://portals.flexicadastre.com/drc/en/',
    #'SS': 'http://portals.flexicadastre.com/southsudan/',
    #'PG': 'http://portal.mra.gov.pg/Map/'
}

# there's been some trouble in the past with regards to the
# greographic reference system used. the settings here
# should emit the closest that ESRI will give you in lieu of
# WGS84 (i.e. Google Maps).
QUERY = {
    'where': '1=1',
    'outFields': '*',
    'geometryType': 'esriGeometryPolygon',
    'spatialRel': 'esriSpatialRelIntersects',
    # 'units': 'esriSRUnit_Meter',
    'outSR': 102100,  # wgs 84
    'resultRecordCount': 500,
    'resultOffset': 0,
    'returnGeometry': 'true',
    'f': 'pjson'
}


def convrow(data):
    # this converts all values in the attribute data to a
    # form suitable for the database storage.
    row = {}
    for name, val in data.items():
        name = name.upper()
        if val is not None and isinstance(val, int):
            if name.startswith('DTE') or name.endswith('_DAT') \
                    or name.endswith('_DATE') or name.endswith('_D') \
                    or name == 'COMPLETED':
                dt = datetime.fromtimestamp(int(val) / 1000)
                val = dt.date().isoformat()
        if name.startswith('GUID'):
            continue
        if name == 'AREA':
            val = min(val, (2 ** 31) - 1)
        if name == 'ID':
            name = 'FC_ID'
        if not len(unicode(val).strip()):
            val = None
        row[slugify(name, sep='_')] = val
    return row


def store_layer_to_db(data, layer, features):
    """Load a layer of features into a database table."""
    # table names are generated from the name of the layer and
    # the name of the country.
    tbl_name = '%s %s' % (data['name'], layer['name'])
    tbl_name = slugify(tbl_name, sep='_')
    log.info('    -> %s: %s rows', tbl_name, len(features))
    tbl = database[tbl_name]
    # clear out all existing data.
    tbl.delete()
    rows = []
    types = {}
    for feature in features:
        row = convrow(feature['attributes'])
        for k, v in row.items():
            if isinstance(v, (int, long)):
                types[k] = BigInteger
        row['layer_name'] = layer['name']
        row['layer_id'] = layer['id']
        row['source_name'] = data['name']
        row['source_title'] = data['title']
        row['source_url'] = data['url']
        if QUERY['returnGeometry'] == 'true':
            # store the geometry as JSON. not sure this is a
            # great idea because it may make the resulting
            # CSV files really hard to parse.
            row['_geometry'] = json.dumps(feature['geometry'])
            row['_attributes'] = json.dumps(feature['attributes'])
        rows.append(row)
    tbl.insert_many(rows, types=types)

    # Dump the table to a CSV file
    csv_file = '%s.csv' % tbl_name
    log.info('    -> %s', csv_file)
    dataset.freeze(tbl, prefix=DATA_PATH, filename=csv_file, format='csv')


def store_layer_to_geojson(data, layer, features):
    """Store the returned data as a GeoJSON file."""
    # skip if we're not loading geometries:
    if QUERY['returnGeometry'] != 'true':
        return

    out = {
        "type": "FeatureCollection",
        "features": []
    }
    for fdata in features:
        attrs = {}
        for k, v in fdata.get('attributes').items():
            k = k.lower().strip()
            attrs[k] = v

        if not fdata.get('geometry', {}).get('rings'):
            continue

        props = dict(attrs)
        props['layer'] = layer.get('name')
        out['features'].append({
            'type': 'Feature',
            'geometry': {
                'type': 'Polygon',
                'coordinates': fdata.get('geometry', {}).get('rings')
            },
            'properties': props
        })

    name = slugify('%s %s' % (data['name'], layer.get('name')), sep='_')
    name = name + '.geojson'
    log.info('    -> %s', name)
    with open(os.path.join(DATA_PATH, name), 'wb') as fh:
        json.dump(out, fh)


def scrape_layers(sess, data, token, rest_url):
    # This is the actual scraping of the ESRI API.
    res = sess.get(rest_url, params={'f': 'json', 'token': token})
    log.info('Scraping %(title)r', data)
    for layer in res.json().get('layers'):
        query_url = '%s/%s/query' % (rest_url, layer['id'])
        q = QUERY.copy()
        q['token'] = token
        log.info('-> Layer: [%(id)s] %(name)s ', layer)
        features = []
        for i in count(0):
            q['resultOffset'] = q['resultRecordCount'] * i
            res = sess.get(query_url, params=q)
            page = res.json()
            features.extend(page['features'])
            if not page.get('exceededTransferLimit'):
                break
        if len(features) < 2:
            log.info('    -> Skip layer, too few rows')
            continue
        store_layer_to_db(data, layer, features)
        store_layer_to_geojson(data, layer, features)


def scrape_configs():
    for name, url in SITES.items():
        sess = requests.Session()
        res = sess.get(url)
        # some ugly stuff to extraxt the access token from the portal
        # site.
        groups = re.search(r"MainPage\.Init\('(.*)'", res.content)
        text = groups.group(1)
        text = text.replace("\\\\\\'", "")
        text = text.replace("\\'", "")
        text = text.replace('\\\\\\"', "")

        text = '"%s"' % text
        cfg = json.loads(json.loads(text))
        token = cfg['Extras'].pop()
        data = {
            'name': name,
            'title': cfg['Title'],
            'url': url
        }
        try:
            for service in cfg['MapServices']:
                if service['MapServiceType'] == 'Features':
                    rest_url = service['RestUrl']
                    scrape_layers(sess, data, token, rest_url)
        except Exception, e:
            log.exception(e)


if __name__ == '__main__':
    scrape_configs()
