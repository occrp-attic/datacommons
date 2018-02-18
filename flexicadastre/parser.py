# coding: utf-8
from __future__ import unicode_literals

import re
from pprint import pprint  # noqa

from flexicadastre.util import convert_data
from flexicadastre.commodities import COMMODITIES

PARTIES_RE = re.compile(r'(.*) \((\s*\d*\s*%?\s*)\)')
PARTIES_FIELDS = ['Parties', 'Applicant', 'Operador', 'Operador_L', 'Company']
COMMODITIES_FIELDS = ['Commodities', 'CommoditiesCd']


def parse_commodities(record):
    commodities = set()
    for field in COMMODITIES_FIELDS:
        value = record.pop(field, None)
        if value is None:
            continue
        for name in value.split(', '):
            if '(' in name:
                name, _ = name.split('(', 1)
            name = name.replace(',', ' ')
            name = name.strip()
            # if name not in COMMODITIES and name not in COMMODITIES.values():
            #     print name
            name = COMMODITIES.get(name, name)
            commodities.add(name)
    return commodities


def parse_parties(record):
    for field in PARTIES_FIELDS:
        value = record.pop(field, None)
        if value is None:
            continue
        for name in value.split(', '):
            match = PARTIES_RE.match(name)
            if match is not None:
                name = match.group(1).strip()
                share = match.group(2).strip()
                yield name, field, share
            else:
                yield name, field, None


def feature(context, data):
    feature = data.pop('feature')
    record_id = feature.get('FeatureId')
    record = convert_data(feature)
    record['PortalTitle'] = data.pop('portal_title')
    record['PortalURL'] = data.pop('portal_url')
    record['LayerName'] = data.pop('name')

    for commodity in parse_commodities(record):
        context.emit(rule='commodity', data={
            'Commodity': commodity,
            'FeatureId': record_id
        })

    parties = 0
    for field, party, share in parse_parties(record):
        context.emit(rule='party', data={
            'Party': party,
            'Share': share,
            'Field': field,
            'FeatureId': record_id
        })
        parties += 1

    if parties == 0:
        pprint(record)

    context.emit(data=record)
