from lxml import etree


def clean_text(text):
    text = text.replace('<![CDATA[', '')
    text = text.replace(']]>', '>')
    text = text.replace('&', '&amp;')
    return text


def convert_commitment(base, commitment):
    rows = []
    common = {}
    common['date'] = commitment.findtext('year')
    common['total'] = commitment.findtext('amount')
    common['cofinancing_rate'] = commitment.findtext('cofinancing_rate')
    common['cofinancing_rate_pct'] = common['cofinancing_rate']
    common['position_key'] = commitment.findtext('position_key')
    common['grant_subject'] = clean_text(commitment.findtext('grant_subject'))
    common['responsible_department'] = clean_text(commitment.findtext(
        'responsible_department'))
    common['action_type'] = commitment.findtext('actiontype')
    budget_line = commitment.findtext('budget_line')

    budget_split = budget_line.rsplit('(', 1)
    name = budget_split[0]
    if len(budget_split) > 1:
        code = budget_split[1]
        code = code.replace(')', '').replace('"', '').strip()
    else:
        code = ''
    common['budget_item'] = name.strip()
    common['budget_code'] = code

    parts = code.split(".")
    common['title'] = parts[0]
    common['chapter'] = '.'.join(parts[:2])
    common['article'] = '.'.join(parts[:3])
    if len(parts) == 4:
        common['item'] = '.'.join(parts[:4])

    for beneficiary in commitment.findall('.//beneficiary'):
        row = common.copy()
        row['beneficiary'] = clean_text(beneficiary.findtext('name'))
        if '*' in row['beneficiary']:
            row['beneficiary'], row['alias'] = row['beneficiary'].split('*', 1)
        else:
            row['alias'] = row['beneficiary']
        row['address'] = clean_text(beneficiary.findtext('address'))
        row['vat_number'] = clean_text(beneficiary.findtext('vat'))
        row['expensetype'] = beneficiary.findtext('expensetype')
        row['city'] = beneficiary.findtext('city')
        row['postcode'] = beneficiary.findtext('post_code')
        row['country'] = beneficiary.findtext('country')
        row['geozone'] = beneficiary.findtext('geozone')
        row['coordinator'] = beneficiary.findtext('coordinator')
        detail_amount = beneficiary.findtext('detail_amount')
        if detail_amount is not None and len(detail_amount):
            row['amount'] = detail_amount
        else:
            row['amount'] = row['total']
        if row['amount'] == "NaN":
            row['amount'] = row['total']

        base['source_id'] += 1
        row.update(base)
        rows.append(row)
    return rows


def parse(context, data):
    base = {'source_url': data.get('url'), 'source_id': 0}
    with context.load_file(data['content_hash'], read_mode='rt') as res:
        xml_data = etree.fromstringlist(res)
        for i, commitment in enumerate(xml_data.findall('.//commitment')):
            base['source_contract_id'] = i
            base['source_line'] = commitment.sourceline
            rows = convert_commitment(base, commitment)
            for row in rows:
                print(row)
                context.emit(data=row)
