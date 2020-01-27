# coding: utf-8
from __future__ import unicode_literals

from pprint import pprint  # noqa
from datetime import datetime
from normality import stringify
import balkhash
from followthemoney import model


REMOVE = ['Shape.STArea()', 'Shape.STLength()', 'Shape.len',
          'SHAPE.len', 'SHAPE.fid' 'FullShapeGeometryWKT',
          'Shape__Length']

RENAME = {
    'SDELiberiaProd.DBO.MLMELicenses_20160119.Area': 'Area',
    'Shape.area': 'Area',
    'SHAPE.area': 'Area',
    'Shape__Area': 'Area',
    'CODE': 'Code',
    'NAME': 'Name',
    'STATUS': 'Status'
}


def convert_data(data):
    # this converts all values in the attribute data to a
    # form suitable for the database storage.
    row = {}
    for name, val in data.items():
        name = RENAME.get(name, name)
        uname = name.upper()
        if val is not None and isinstance(val, int):
            if uname.startswith('DTE') or uname.endswith('_DAT') \
                    or uname.endswith('_DATE') or uname.endswith('_D') \
                    or uname == 'COMPLETED':
                dt = datetime.fromtimestamp(int(val) / 1000)
                val = dt.date().isoformat()
        if uname.startswith('GUID'):
            continue
        if name in REMOVE:
            continue
        if uname == 'AREA':
            if isinstance(val, str):
                val = val.split(' ')[0]
            val = min(int(val), (2 ** 31) - 1)
        val = stringify(val)
        if val is not None:
            row[name] = val
    return row


class EntityEmitter(object):

    def __init__(self, context):
        self.fragment = 0
        self.log = context.log
        self.name = context.crawler.name
        self.dataset = balkhash.init(self.name)
        self.bulk = self.dataset.bulk()

    def make(self, schema):
        entity = model.make_entity(schema, key_prefix=self.name)
        return entity

    def emit(self, entity, rule='pass'):
        if entity.id is None:
            raise RuntimeError("Entity has no ID: %r", entity)
        self.bulk.put(entity, fragment=str(self.fragment))
        self.fragment += 1

    def finalize(self):
        self.bulk.flush()
