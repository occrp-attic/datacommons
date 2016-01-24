# coding: utf-8
import os
import json
import glob
import hashlib
from normality import slugify
from datetime import datetime
from pprint import pprint # noqa
from jinja2 import Template

from common import DATA_PATH, database

TODAY = datetime.utcnow().date().isoformat()
KEY_FIELDS = ['source_url', 'layer_name', 'guidlicense', 'guidpart',
              'objectid', 'objectid_1', 'fid']
SOURCE_PATH = os.path.join(DATA_PATH, 'raw')
try:
    os.makedirs(SOURCE_PATH)
except:
    pass

changes_table = database['flexi_changes']


def parse_features():
    for file_path in glob.glob(os.path.join(SOURCE_PATH, '*')):
        if 'BW.json' not in file_path:
            continue
        print file_path
        with open(file_path, 'rb') as fh:
            ctx = json.load(fh)

        layers = ctx.pop('layers')
        for layer in layers:
            lctx = ctx.copy()
            lctx['layer_name'] = layer['name']
            lctx['layer_id'] = layer['id']

            for feature in layer['data']['features']:
                attrs = feature.get('attributes')
                attrs.update(lctx)
                data = {}
                for k, v in attrs.items():
                    data[slugify(k, sep='_')] = v
                yield data


def generate_changes():
    changes = []
    keys = set()
    for feat in parse_features():
        key = hashlib.sha1()
        fp = hashlib.sha1()
        for k in sorted(feat.keys()):
            v = feat.get(k)
            if v is None:
                continue

            if k in KEY_FIELDS:
                key.update(unicode(v).encode('utf-8'))
            else:
                fp.update(unicode(v).encode('utf-8'))

        fp = fp.hexdigest()
        key = key.hexdigest()
        keys.add(key)
        record = {
            'date': TODAY,
            'data': json.dumps(feat),
            'expired': None,
            'key': key,
            'fp': fp
        }

        previous = list(changes_table.find(key=key))
        if len(previous):
            previous = sorted(previous, key=lambda c: c.get('date'))
            latest = previous.pop()

            if latest.get('fp') == fp:
                continue

            changes.append({
                'record_new': feat,
                'record_old': json.loads(latest.get('data')),
                'date_new': TODAY,
                'date_old': latest.get('date'),
                'operation': 'change'
            })
        else:
            changes.append({
                'record_new': feat,
                'record_old': {},
                'date_new': TODAY,
                'date_old': None,
                'operation': 'add'
            })
        changes_table.insert(record, ['key', 'fp'])
        # print record

    for old in changes_table.find(expired=None):
        if old.get('key') not in keys:
            changes.append({
                'record_new': {},
                'record_old': json.loads(old.get('data')),
                'date_new': TODAY,
                'date_old': old.get('date'),
                'operation': 'remove'
            })
            old['expired'] = TODAY
            changes_table.update(old, ['key', 'fp'])

    return changes


def render_changes(changes):
    with open('notification.jinja.html', 'r') as fh:
        template = Template(fh.read())

    sources = {}
    for change in changes:
        data = change.get('record_new') or change.get('record_old')

        source_url = data.get('source_url')
        if source_url not in sources:
            sources[source_url] = {
                'url': source_url,
                'title': data.get('source_title'),
                'layers': {}
            }

        layer_id = data.get('layer_id')
        if layer_id not in sources[source_url]['layers']:
            csv_name = '%(source_name)s %(layer_name)s' % data
            csv_name = slugify(csv_name, sep='_')
            csv_name = 'http://data.pudo.org/flexicadastre/csv/%s.csv' % csv_name

            sources[source_url]['layers'][layer_id] = {
                'id': layer_id,
                'title': data.get('layer_name'),
                'csv': csv_name,
                'changes': []
            }

        change['headers'] = change.get('record_new').keys()
        change['headers'].extend(change.get('record_old').keys())

        for obj in [change.get('record_new'), change.get('record_old')]:
            obj.pop('layer_id', None)
            obj.pop('layer_name', None)
            obj.pop('source_title', None)
            obj.pop('source_name', None)
            obj.pop('source_url', None)

        sources[source_url]['layers'][layer_id]['changes'].append(change)

    out = template.render(sources=sources)
    with open(os.path.join(DATA_PATH, 'report_%s.html' % TODAY), 'w') as fo:
        fo.write(out.encode('utf-8'))


if __name__ == '__main__':
    changes = generate_changes()
    text = render_changes(changes)
