from lxml import html
from pprint import pprint  # noqa
from urllib.parse import unquote, urljoin
from normality import slugify
from dateutil.parser import parse as parse_date

from memorious.helpers import ViewForm

URL = 'https://www.greg.gg/webCompSearch.aspx'
HEADERS = {'X-Requested-With': 'XMLHttpRequest'}


def search(context, data):
    context.http.reset()
    result = context.http.get(URL)
    form = ViewForm.from_result(result)
    form['ctl00$cntPortal$ddlRegister'] = 0
    form['ctl00$cntPortal$txtCompRegNum'] = data.get('number')
    form['ctl00$ScriptManager2'] = 'ctl00$cntPortal$updPanel|ctl00$cntPortal$btnSearch'  # noqa
    form['ctl00$cntPortal$btnSearch'] = 'Search'
    form['__EVENTTARGET'] = ''
    form['__EVENTARGUMENT'] = ''
    form['__ASYNCPOST'] = 'true'
    form.clear('ctl00$btnLogin')
    form.clear('ctl00$btnDiscAccept')
    form.clear('ctl00$cntPortal$chkPrevNames')
    result = context.http.post(URL, data=form, headers=HEADERS)
    url_data = form.copy()
    html_result = None
    sections = result.text.split('|')
    for tpl in zip(*[iter(sections[2:])] * 4):
        a, b, c, d = tpl
        if b == 'hiddenField':
            url_data[c] = d
        for fld in tpl:
            if 'grdSearchResults' in fld:
                html_result = fld
    if html_result is None:
        return
    doc = html.fromstring(html_result)
    for row in doc.findall('.//tr'):
        cells = row.findall('.//td')
        if len(cells) < 3:
            continue
        call = cells[0].find('./input').get('name')
        data = url_data.copy()
        data[call + '.x'] = 9
        data[call + '.y'] = 5
        data['ctl00$ScriptManager2'] = 'ctl00$cntPortal$updPanel|' + call
        res = context.http.post(URL, data=data, headers=HEADERS)
        url_part = unquote(res.text.split('|')[-2])
        if 'webCompSearchDetails' not in url_part:
            context.emit_warning("Invalid URL snippet: %r" % url_part)
            continue
        url = urljoin(URL, url_part)
        context.emit(data={'url': url, 'number': data.get('number')})


def parse(context, data):
    table = context.datastore[context.params.get('table')]
    with context.http.rehash(data) as result:
        company = {}
        header = None
        for td in result.html.findall('.//td'):
            if td.get('class') == 'main_text_alt':
                header = slugify(td.text, sep='_')
                continue
            if header is None:
                continue
            company[header] = td.text_content().strip()
            header = None
        company.pop('purchase_documents', None)
        company['source_url'] = result.url
        company['number'] = data.get('number')
        company = clean_data(context, company)
        context.log.info("Parsed: %(name)s", company)
        table.delete(number=company['source_url'])
        table.insert(company)


def clean_data(context, data):
    name = data.get('company_name') \
        or data.get('llp_name') \
        or data.get('foundation_name')
    if name is not None and '(as at' in name:
        name, _ = name.split('(as at', 1)
    if name is not None:
        name = name.strip()
    data['name'] = name
    date = data.get('company_registered_date') \
        or data.get('llp_registration_date') \
        or data.get('foundation_registration_date')
    try:
        data['registered_date'] = parse_date(date).date()
    except Exception:
        context.emit_warning('Cannot parse date: %s' % date)
    status = data.get('company_status') \
        or data.get('llp_status') \
        or data.get('foundation_status')
    data['status'] = status
    if status is not None and '(as at' in status:
        status, last_change = status.split('(as at', 1)
        status = status.strip()
        last_change = last_change.strip(')').strip()
        try:
            data['last_change'] = parse_date(last_change).date()
        except Exception:
            context.emit_warning('Cannot parse date: %s' % last_change)
    return data
