import re
from pprint import pprint  # noqa
from ftmstore.memorious import EntityEmitter

from datacommons.crawlers.flexicadastre.util import convert_data  # noqa
from datacommons.crawlers.flexicadastre.commodities import COMMODITIES

PARTIES_RE = re.compile(r"(.*) \((\s*\d*([\.,]\d+)\s*%?\s*)\)")
PARTIES_FIELDS = [
    "Parties",
    "Applicant",
    "Operador",
    "Operador_L",
    "Company",
    "Lic_Holder",
    "CONSORTIUM",
]
COMMODITIES_FIELDS = ["Commodities", "CommoditiesCd", "Commodity"]


def parse_commodities(record):
    commodities = set()
    for field in COMMODITIES_FIELDS:
        value = record.pop(field, None)
        if value is None:
            continue
        for name in value.split(", "):
            if "(" in name:
                name, _ = name.split("(", 1)
            name = name.replace(",", " ")
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
        for name in value.split(", "):
            match = PARTIES_RE.match(name)
            if match is not None:
                name = match.group(1).strip()
                share = match.group(2).strip()
                yield field, name, share
            else:
                yield field, name, None


def feature(context, data):
    feature = data.pop("feature")
    record_id = feature.get("FeatureId")
    record = convert_data(feature)
    record["PortalTitle"] = data.pop("portal_title")
    record["PortalURL"] = data.pop("portal_url")
    record["LayerName"] = data.pop("name")
    record["Interest"] = record.get("Interest")
    record["Area"] = record.get("Area")

    emitter = EntityEmitter(context)
    commodities = parse_commodities(record)
    concession = emitter.make("License")
    concession.make_id(record_id)
    concession.add("name", record.get("Type"))
    concession.add("type", [record.get("Type"), record.get("TypeGroup")])
    concession.add("country", record.get("Jurisdic"))
    concession.add("sourceUrl", record.get("PortalURL"))
    concession.add("description", record.get("Interest"))
    concession.add("amount", record.get("AreaValue"))
    area = record.get("Area")
    area_unit = record.get("AreaUnit")
    if area_unit is not None:
        area = "%s %s" % (area, area_unit)
    concession.add("area", area)
    concession.add("commodities", commodities)
    concession.add("notes", record.get("Comments"))
    emitter.emit(concession)

    parties = 0
    for field, party, share in parse_parties(record):
        entity = emitter.make("LegalEntity")
        entity.make_id(party)
        entity.add("name", party)
        entity.add("sourceUrl", record.get("PortalURL"))
        ownership = emitter.make("Ownership")
        ownership.make_id(record_id, party, share)
        ownership.add("owner", entity)
        ownership.add("asset", concession)
        ownership.add("status", record.get("Status"))
        ownership.add("percentage", share)
        ownership.add("startDate", record.get("DteGranted"))
        ownership.add("endDate", record.get("DteExpires"))
        ownership.add("sourceUrl", record.get("PortalURL"))
        ownership.add("recordId", record.get("Code"))
        emitter.emit(entity)
        emitter.emit(ownership)

        parties += 1

    if parties == 0:
        context.emit_warning("No parties: %s - %s" % (record["LayerName"], record_id))

    emitter.finalize()
