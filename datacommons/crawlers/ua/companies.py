# -*- coding: utf-8 -*-

from lxml import etree


FOUNDER_SPLIT = u", розмір"


def parse(context, data):
    if "UO" in data["file_name"]:
        parse_companies_file(context, data)
    elif "FOP" in data["file_name"]:
        parse_entrepreneurs_file(context, data)
    else:
        raise RuntimeError("Unexpected file %s" % data["file_name"])


def parse_companies_file(context, data):
    with context.load_file(data["content_hash"]) as fp:
        ctx = etree.iterparse(fp, events=("end",), tag="RECORD", encoding="utf-8")
        for event, company in ctx:
            record = {
                "name": company.find("NAME").text,
                "short_name": company.find("SHORT_NAME").text,
                "edrpou": company.find("EDRPOU").text,
                "address": company.find("ADDRESS").text,
                "boss": company.find("BOSS").text,
                "kved": company.find("KVED").text,
                "stan": company.find("STAN").text,
            }
            context.emit(rule="store_company", data=record)
            for founder in company.findall("FOUNDERS/FOUNDER"):
                founder = founder.text.strip()
                name, amount = None, None
                if FOUNDER_SPLIT in founder:
                    name, amount = founder.rsplit(FOUNDER_SPLIT, 1)
                    if " - " in amount:
                        _, amount = amount.rsplit(" - ", 1)
                    amount = amount.replace(u" грн.", "")
                context.emit(
                    rule="store_founder",
                    data={
                        "company_name": record["name"],
                        "company_edrpou": record["edrpou"],
                        "founder": founder,
                        "name": name or founder,
                        "amount": amount,
                    },
                )


def parse_entrepreneurs_file(context, data):
    with context.load_file(data["content_hash"]) as fp:
        ctx = etree.iterparse(fp, events=("end",), tag="RECORD")
        for event, elem in ctx:
            record = {
                "full_name": elem.find("FIO"),
                "address": elem.find("ADDRESS"),
                "main_activity": elem.find("KVED"),
                "state": elem.find("STAN"),
            }
            for item in record:
                if record[item] is not None:
                    record[item] = record[item].text
            context.emit(rule="store_entrepreneur", data=record)
