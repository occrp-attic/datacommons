# coding: utf-8
import csv
import traceback
from pprint import pprint  # noqa

from lxml import etree

from collections import defaultdict
from memorious.helpers import parse_date


RECORD_URL = "http://wwwinfo.mfcr.cz/cgi-bin/ares/darv_vreo.cgi?ico="


def parse_csv(context, data):
    with context.load_file(data['content_hash']) as fp:
        rows = csv.reader(fp, delimiter=';')
        ids = [row[0] for row in rows]
    for id_ in ids:
        data = {"url": RECORD_URL + id_}
        context.emit(data=data)


def address_extract(root, namespace, prefix=''):
    if root is None:
        return {}
    components = (
        'ruianKod',  # cadaster code
        'stat',  # state
        'psc',  # postal code
        'okres',  # district
        'obec',  # village
        'ulice',  # street
        'cisloTxt',  # street number
    )
    # defaultdict so that address_text formatting will degrade
    # gracefully with missing data
    result = defaultdict(str)
    for label in components:
        match = root.find('are:%s' % label, namespace)
        if match is not None:
            result[label] = match.text
    result['full'] = '%(ulice)s %(cisloTxt)s, %(psc)s %(obec)s' % result
    return {'%s%s' % (prefix, k): v for k, v in result.items()}


def store(context, data):
    table_name = context.params.get("table")
    company_table = context.datastore[table_name]
    members_table = context.datastore[table_name + "_members"]
    with context.load_file(data['content_hash']) as fh:
        xmlfile = etree.parse(fh)

        try:
            namespace = xmlfile.getroot().nsmap
            company_id_el = xmlfile.find(".//are:ICO", namespace)
            if company_id_el is None:
                return
            company_id = company_id_el.text
            company = xmlfile.find(".//are:ObchodniFirma", namespace).text
            purpose_paras = xmlfile.findall(
                './/are:PredmetPodnikani/are:Text', namespace)
            company_objective = '\n'.join(x.text for x in purpose_paras)
            company_address = address_extract(
                xmlfile.find('.//are:Sidlo', namespace),
                namespace,
                'address_')
            registration_date = parse_date(
                xmlfile.find(".//are:DatumZapisu", namespace).text
            )
            company_data = {
                "company_id": company_id,
                "company": company,
                "company_objective": company_objective,
                "registration_date": registration_date,
            }
            company_data.update(company_address)
            company_table.upsert(company_data, ["company_id"])
            for member in xmlfile.findall(".//are:Clen", namespace):
                member_name = ''
                first_name = member.find("are:fosoba/are:jmeno", namespace)
                surname = member.find("are:fosoba/are:prijmeni", namespace)
                first_name_text = ''
                if first_name is not None:
                    first_name_text = first_name.text
                surname_text = surname.text if surname is not None else ''
                if first_name_text:
                    member_name += first_name_text
                if surname_text:
                    member_name += " " + surname_text
                member_role = member.find("are:funkce/are:nazev", namespace)
                if member_role is not None:
                    member_role = member_role.text
                member_address = address_extract(
                    member.find('.//are:adresa', namespace),
                    namespace,
                    'address_')

                if member_name or member_role:
                    member_data = {
                        "member_name": member_name,
                        "member_role": member_role,
                        "company_id": company_id,
                        "company": company,
                        "firstName": first_name_text,
                        "lastName": surname_text,
                        "startDate": member.get('dza'),
                        "endDate": member.get('dvy')
                    }
                    member_data.update(member_address)
                    keys = [
                        "member_name",
                        "member_role",
                        "company_id",
                        "startDate"
                    ]
                    members_table.upsert(member_data, keys)
        except Exception:
            context.emit_warning('failed to process %s' %
                                 data.get('url', 'unknown url'))
            context.emit_warning(traceback.format_exc())
