import re
import os
import json
import logging
import requests
import dataset
# from pprint import pprint
from normality import slugify
from datetime import datetime
from unicodecsv import DictWriter
from itertools import count
from morphium import Archive, env

log = logging.getLogger('flexicadastre')
db_uri = env('DATABASE_URI', 'sqlite:///data.sqlite')
archive = Archive(bucket='archive.pudo.org', prefix='flexicadastre')
data_path = env('DATA_PATH', 'data')
database = dataset.connect(db_uri)
index_table = database['data']

# comment out countries here to disable scraping the
# respective countries.
SITES = {
    # 'BW': 'http://portals.flexicadastre.com/botswana/',
    'UG': 'http://portals.flexicadastre.com/uganda/',
    'NA': 'http://portals.flexicadastre.com/Namibia/',
    'MZ': 'http://portals.flexicadastre.com/mozambique/en/',
    'LR': 'http://portals.flexicadastre.com/liberia/',
    'KE': 'https://portal.miningcadastre.go.ke/mapportal/',
    'RW': 'http://portals.flexicadastre.com/rwanda/',
    'TZ': 'http://portal.mem.go.tz/map/',
    'ZM': 'http://portals.flexicadastre.com/zambia/',
    'CD': 'http://portals.flexicadastre.com/drc/en/',
    'SS': 'http://portals.flexicadastre.com/southsudan/',
    'PG': 'http://portal.mra.gov.pg/Map/'
}

IGNORE = ['rw_25ha_grid', 'mz_moz_geol', 'ug_north']

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


def store_layer_to_csv(res_name, data, layer, features):
    """Load a layer of features into a database table."""
    # table names are generated from the name of the layer and
    # the name of the country.
    csv_path = os.path.join(data_path, '%s.csv' % res_name)
    log.info('CSV: %s: %s rows', csv_path, len(features))

    with open(csv_path, 'w') as fh:
        writer = None
        for feature in features:
            row = convrow(feature['attributes'])
            row['layer_name'] = layer['name']
            row['layer_id'] = layer['id']
            row['source_name'] = data['name']
            row['source_title'] = data['title']
            row['source_url'] = data['url']

            # store the geometry as JSON. not sure this is a
            # great idea because it may make the resulting
            # CSV files really hard to parse.
            # row['_geometry'] = json.dumps(feature['geometry'])

            if writer is None:
                writer = DictWriter(fh, row.keys())
                writer.writeheader()

            writer.writerow(row)

    url = archive.upload_file(csv_path)
    os.unlink(csv_path)
    return url


def store_layer_to_geojson(res_name, data, layer, features):
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

    file_path = os.path.join(data_path, '%s.geo.json' % res_name)
    log.info('GeoJSON: %s', file_path)
    with open(file_path, 'wb') as fh:
        json.dump(out, fh)

    url = archive.upload_file(file_path)
    os.unlink(file_path)
    return url


def scrape_layers(sess, data, token, rest_url):
    # This is the actual scraping of the ESRI API.
    res = sess.get(rest_url, params={'f': 'json', 'token': token})
    log.info('Scraping %(title)r', data)
    for layer in res.json().get('layers'):
        res_name = '%s %s' % (data['name'], layer['name'])
        res_name = slugify(res_name, sep='_')
        log.info('Layer: [%s] %s (%s) ', layer['id'], layer['name'], res_name)

        if res_name in IGNORE:
            log.info("[%(name)s] Skip (blacklisted)", layer)
            continue

        query_url = '%s/%s/query' % (rest_url, layer['id'])
        q = QUERY.copy()
        q['token'] = token
        features = []
        for i in count(0):
            q['resultOffset'] = q['resultRecordCount'] * i
            res = sess.get(query_url, params=q)
            page = res.json()
            features.extend(page.get('features', []))
            if not page.get('exceededTransferLimit'):
                break

        csv_url = store_layer_to_csv(res_name, data, layer, features)
        json_url = store_layer_to_geojson(res_name, data, layer, features)
        index_table.upsert({
            'resource': res_name,
            'tag': archive.tag,
            'csv_url': csv_url,
            'json_url': json_url,
            'features': len(features),
            'layer_name': layer['name'],
            'layer_id': layer['id'],
            'source_name': data['name'],
            'source_title': data['title'],
            'source_url': data['url']
        }, ['resource'])


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
    try:
        os.makedirs(data_path)
    except Exception:
        pass
    scrape_configs()
