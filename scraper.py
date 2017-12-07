import re
import os
import json
import logging
import requests
import dataset
from pprint import pprint  # noqa
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
    'gn_geologique',
    'gn_forests',
    'gn_country_border',
    'gn_regions',
    'mz_fronteiras_nacionais',
    'mz_areas_interditas',
    'mz_moz_geol',
    'na_geology',
    'na_withdrawn_areas',
    'na_environmentally_sensitive_areas',
    'na_country',
    'na_farms',
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
    'ug_protected_areas',
    'pg_districts',
    'pg_local_level_governments',
    'pg_country',
    'pg_reserved_30_day_areas',
    'pg_provinces',
    'rw_protected_areas',
    'rw_national_parks',
    'rw_country',
    'rw_provinces',
    'rw_districts',
    'rw_sectors',
    'rw_cells',
    'rw_25ha_grid',
    'rw_100ha_grid',
    'rw_400ha_grid',
    'ss_geology',
    'ss_geology200m',
    'ss_wildlife_conservation_areas',
    'ss_country_boundary_250m_restriction',
    'ss_country_boundary',
    'ss_lakes_marshlands',
    'cd_drc_geology',
    'cd_grid',
    'tz_geology',
    'tz_protected_areas',
    'tz_demarcated_areas',
    'tz_districts',
    'tz_boundary',
    'zm_geology',
    'zm_national_parks',
    'zm_boundary',
    'ci_parc_national',
    'ci_frontires',
    'mw_malawi',
    'mw_malawiborder_webmerc'
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


def load_features(url, token, seen, extent):
    q = QUERY.copy()
    if token is not None:
        q['token'] = token
    q['geometry'] = json.dumps(extent)
    res = requests.get(url, params=q)
    page = res.json()
    for feature in page.get('features', []):
        attrs = feature.get('attributes')
        obj = attrs.get('guidPart')
        obj = obj or attrs.get('OBJECTID_1')
        obj = obj or attrs.get('OBJECTID_12')
        obj = obj or attrs.get('OBJECTID')
        obj = obj or attrs.get('ESRI_OID')
        if obj is None:
            log.info("Missing ID: %r", attrs.keys())
        if obj not in seen:
            seen.add(obj)
            yield attrs
    if page.get('exceededTransferLimit'):
        for child in split_envelope(extent):
            for attrs in load_features(url, token, seen, child):
                yield attrs


def scrape_layer(data, token, rest_url, extent, layer):
    res_name = '%s %s' % (data['name'], layer['name'])
    res_name = slugify(res_name, sep='_')
    csv_path = os.path.join(data_path, '%s.csv' % res_name)
    log.info('Layer: [%s] %s (%s) ', layer['id'], layer['name'], csv_path)

    if res_name in IGNORE or res_name.startswith('pg_geol'):
        log.info("[%(name)s] Skip (blacklisted)", layer)
        return

    query_url = '%s/%s/query' % (rest_url, layer['id'])
    rows = 0
    with open(csv_path, 'w') as fh:
        writer = None
        for feature in load_features(query_url, token, set(), extent):
            row = convrow(feature)
            row['layer_name'] = layer['name']
            row['layer_id'] = layer['id']
            row['source_name'] = data['name']
            row['source_title'] = data['title']
            row['source_url'] = data['url']
            rows = rows + 1

            if writer is None:
                writer = DictWriter(fh, row.keys())
                writer.writeheader()

            writer.writerow(row)

    if rows > 1:
        url = archive.upload_file(csv_path)
        index_table.upsert({
            'resource': res_name,
            'tag': archive.tag,
            'csv_url': url,
            'features': rows,
            'layer_name': layer['name'],
            'layer_id': layer['id'],
            'source_name': data['name'],
            'source_title': data['title'],
            'source_url': data['url']
        }, ['resource'])

    os.unlink(csv_path)


def scrape_layers(data, token, rest_url):
    # This is the actual scraping of the ESRI API.
    log.info('Scraping: %(title)s', data)
    params = {
        'f': 'json'
    }
    if token is not None:
        params['token'] = token
    res = requests.get(rest_url, params=params)
    site_info = res.json()
    extent = site_info['fullExtent']
    for layer in site_info.get('layers'):
        scrape_layer(data, token, rest_url, extent, layer)


def scrape_configs():
    for name, url in SITES.items():
        res = requests.get(url)
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
                    token = token or service.get('ArcGISToken')
                    scrape_layers(data, token, service['RestUrl'])
        except Exception, e:
            log.exception(e)


if __name__ == '__main__':
    try:
        os.makedirs(data_path)
    except Exception:
        pass
    scrape_configs()
