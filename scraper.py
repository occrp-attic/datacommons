import re
import os
import json
import logging
import requests
import dataset
from pprint import pprint
from normality import slugify
from datetime import datetime
from unicodecsv import DictWriter
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
    'UG': 'http://portals.flexicadastre.com/uganda/',
    'NA': 'http://portals.flexicadastre.com/Namibia/',
    'MZ': 'http://portals.flexicadastre.com/mozambique/en/',
    'MW': 'http://portals.flexicadastre.com/malawi/',
    'LR': 'http://portals.flexicadastre.com/liberia/',
    'KE': 'https://portal.miningcadastre.go.ke/mapportal/',
    'RW': 'http://portals.flexicadastre.com/rwanda/',
    'TZ': 'http://portal.mem.go.tz/map/',
    'ZM': 'http://portals.flexicadastre.com/zambia/',
    'CD': 'http://portals.flexicadastre.com/drc/en/',
    'CI': 'http://portals.flexicadastre.com/cotedivoire/',
    'GN': 'http://guinee.cadastreminier.org/',
    'SS': 'http://portals.flexicadastre.com/southsudan/',
    'PG': 'http://portal.mra.gov.pg/Map/'
}

IGNORE = [
    'ke_parks_and_reserves',
    'ke_forests',
    'ke_counties',
    'ke_geology',
    'ke_boundary',
    'lr_protected_areas',
    'lr_liberia_border',
    'lr_counties',
    'ug_border',
    'ug_north',
    'ug_south',
    'ug_protected_areas'
]

# there's been some trouble in the past with regards to the
# greographic reference system used. the settings here
# should emit the closest that ESRI will give you in lieu of
# WGS84 (i.e. Google Maps).
QUERY = {
    'where': '1=1',
    'outFields': '*',
    'geometryType': 'esriGeometryEnvelope',
    # 'geometryType': 'esriGeometryPolygon',
    'spatialRel': 'esriSpatialRelIntersects',
    # 'units': 'esriSRUnit_Meter',
    # 'outSR': 102100,  # wgs 84
    # 'resultRecordCount': 500,
    # 'resultOffset': 0,
    # 'returnGeometry': 'true',
    'returnGeometry': 'false',
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
    csv_path = os.path.join(data_path, '%s.csv' % res_name)
    log.info('CSV: %s: %s rows', csv_path, len(features))

    with open(csv_path, 'w') as fh:
        writer = None
        for feature in features:
            row = convrow(feature)
            row['layer_name'] = layer['name']
            row['layer_id'] = layer['id']
            row['source_name'] = data['name']
            row['source_title'] = data['title']
            row['source_url'] = data['url']

            if writer is None:
                writer = DictWriter(fh, row.keys())
                writer.writeheader()

            writer.writerow(row)
    url = archive.upload_file(csv_path)
    # os.unlink(csv_path)
    return url


def split_envelope(env):
    xmin = env['xmin']
    xlen = env['xmax'] - env['xmin']
    xhalf = xlen / 2.0
    ymin = env['ymin']
    ylen = env['ymax'] - env['ymin']
    yhalf = ylen / 2.0

    yield {
        'spatialReference': env['spatialReference'],
        'xmax': xmin + xhalf,
        'xmin': xmin,
        'ymax': ymin + yhalf,
        'ymin': ymin,
    }
    yield {
        'spatialReference': env['spatialReference'],
        'xmax': xmin + xlen,
        'xmin': xmin + xhalf,
        'ymax': ymin + yhalf,
        'ymin': ymin,
    }
    yield {
        'spatialReference': env['spatialReference'],
        'xmax': xmin + xhalf,
        'xmin': xmin,
        'ymax': ymin + ylen,
        'ymin': ymin + yhalf,
    }
    yield {
        'spatialReference': env['spatialReference'],
        'xmax': xmin + xlen,
        'xmin': xmin + xhalf,
        'ymax': ymin + ylen,
        'ymin': ymin + yhalf,
    }


def load_features(url, token, extent):
    q = QUERY.copy()
    if token is not None:
        q['token'] = token
    features = {}
    q['geometry'] = json.dumps(extent)
    res = requests.get(url, params=q)
    page = res.json()
    for feature in page.get('features', []):
        attrs = feature.get('attributes')
        obj = attrs.get('OBJECTID_1')
        obj = obj or attrs.get('OBJECTID')
        obj = obj or attrs.get('ESRI_OID')
        if obj is None:
            pprint(attrs)
        else:
            features[obj] = attrs
    if page.get('exceededTransferLimit'):
        for child in split_envelope(extent):
            fs = load_features(url, token, child)
            features.update(fs)
    return features


def scrape_layers(sess, data, token, rest_url):
    # This is the actual scraping of the ESRI API.
    log.info('Scraping: %(title)s', data)
    params = {
        'f': 'json'
    }
    if token is not None:
        params['token'] = token
    res = sess.get(rest_url, params=params)
    layer = res.json()
    extent = layer['fullExtent']
    for layer in res.json().get('layers'):
        res_name = '%s %s' % (data['name'], layer['name'])
        res_name = slugify(res_name, sep='_')
        log.info('Layer: [%s] %s (%s) ', layer['id'], layer['name'], res_name)

        if res_name in IGNORE:
            log.info("[%(name)s] Skip (blacklisted)", layer)
            continue

        query_url = '%s/%s/query' % (rest_url, layer['id'])
        features = load_features(query_url, token, extent)
        features = features.values()

        if not len(features):
            log.info("[%(name)s] Empty", layer)
            continue

        csv_url = store_layer_to_csv(res_name, data, layer, features)

        index_table.upsert({
            'resource': res_name,
            'tag': archive.tag,
            'csv_url': csv_url,
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

        extras = cfg.get('Extras')
        token = None
        if extras and len(extras):
            token = extras.pop()

        data = {
            'name': name,
            'title': cfg['Title'],
            'url': url
        }
        try:
            for service in cfg['MapServices']:
                if service['MapServiceType'] in ['Dynamic', 'Features']:
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
