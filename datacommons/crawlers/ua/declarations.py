import bz2
import json
import re
from decimal import Decimal
from servicelayer.cache import make_key

# from pprint import pprint


CAMEL_RE = re.compile(r"([A-Z]+)")

STEPS_TYPE_1 = {
    "step_2": "relatives",
    "step_3": "real_estate",
    "step_4": "real_estate_construction",
    "step_5": "movable_property",
    "step_6": "vehicles",
    "step_7": "securities",
    "step_8": "corporate_rights",
    "step_9": "legal_entities",
    "step_10": "intangible_assets",
    "step_11": "revenues",
    "step_12": "cash_assets",
    "step_13": "financial_obligations",
    "step_14": "expenses",
    "step_15": "other_jobs",
    "step_16": "memberships",
}


STEPS_TYPE_2 = {
    "step_2": "revenues",
    "step_3": "real_estate",
    "step_4": "vehicles",
    "step_5": "securities",
    "step_6": "corporate_rights",
    "step_7": "movable_property",
    "step_10": "intangible_assets",
}


def audit(context, item):
    "Check if item contains: any nested objects or decimals; and print them."
    out = {}
    for key, value in item.items():
        if isinstance(value, dict):
            out[key] = value
        if isinstance(value, list):
            item[key] = " ".join(str(value)).replace("\n", " ")
        if isinstance(value, Decimal):
            item[key] = str(value)
        if isinstance(value, bool):
            item[key] = str(value.real)
    if len(out):
        context.log.warning("Item %s contains nested objects." % item)


def konvert(obj):
    """Convert to snakecase"""
    if isinstance(obj, dict):
        converted = {}
        for key, value in obj.items():
            key = CAMEL_RE.sub(r"_\1", key).lower()
            key = key.strip("_")
            key = key.replace("-", "_")
            converted[key] = konvert(value)
        return converted
    elif isinstance(obj, list):
        return [konvert(o) for o in obj]
    return obj


def parse_declaration(context, decl):
    decl = konvert(decl)
    infocard = decl.pop("infocard", "")
    declaration_id = infocard.get("id", "")
    full_name = " ".join(
        [
            infocard.get("last_name", ""),
            infocard.get("first_name", ""),
            infocard.get("patronymic", ""),
        ]
    )
    person_id = infocard.get("id", make_key(full_name, decl.get("position", "")))
    year = infocard.get("declaration_year", "")
    url = infocard.get("url", "")
    unified_source = decl.get("unified_source", {})

    if unified_source.get("step_0", {}).get("declaration_type", ""):
        STEPS = STEPS_TYPE_1
    else:
        STEPS = STEPS_TYPE_2

    for num, step in STEPS.items():
        data = unified_source.get(num, {})
        if num == "step_16":
            organizations = data.get("org", {})
            bodies = data.get("part_org", {})
            if isinstance(organizations, dict):
                for idn, org in organizations.items():
                    org["declaration_id"] = declaration_id
                    org["id"] = idn
                    org["url"] = url
                    context.emit(rule="memberships", data=org)
            if isinstance(bodies, dict):
                for idn, body in bodies.items():
                    body["declaration_id"] = declaration_id
                    body["id"] = idn
                    body["url"] = url
                    context.emit(rule="memberships", data=body)
            continue
        for idn, smth in data.items():
            smth["declaration_id"] = declaration_id
            smth["id"] = idn
            smth["url"] = url
            rights = smth.pop("rights", {})
            guarantors = smth.pop("guarantor", {})
            guarantor_realty = smth.pop("guarantor_realty", {})
            if isinstance(rights, dict):
                for r_id, right in rights.items():
                    right["object_id"] = idn
                    right["id"] = r_id
                    right["declaration_id"] = declaration_id
                    audit(context, right)
                    context.emit(rule=STEPS.get(num) + "_rights", data=right)
            if isinstance(guarantors, dict):
                for g_id, guarantor in guarantors.items():
                    guarantor["object_id"] = idn
                    guarantor["id"] = g_id
                    guarantor["declaration_id"] = declaration_id
                    audit(context, guarantor)
                    context.emit(rule=STEPS.get(num) + "_guarantors", data=guarantor)
            if isinstance(guarantor_realty, dict):
                for g_id, grealty in guarantor_realty.items():
                    grealty["object_id"] = idn
                    grealty["id"] = g_id
                    grealty["declaration_id"] = declaration_id
                    audit(context, grealty)
                    context.emit(
                        rule=STEPS.get(num) + "_guarantor_realty", data=grealty
                    )
            audit(context, smth)
            context.emit(rule=STEPS.get(num), data=smth)
    audit(context, infocard)
    context.emit(rule="person", data=infocard)
    context.log.info("Loaded person: [%s] %s" % (person_id, full_name))


def parse(context, data):
    with context.load_file(data["content_hash"]) as fh:
        with bz2.open(fh, mode="rt", encoding="utf-8") as bz2_file:
            for line in bz2_file:
                parse_declaration(context, json.loads(str(line)))
