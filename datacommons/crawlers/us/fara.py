# import time
from banal import ensure_list, is_mapping
from memorious.helpers.key import make_id

# https://www.fara.gov/search.html
# https://efile.fara.gov/ords/f?p=107:3:0::NO:::
STATES = ('Active', 'Terminated')
REGISTRANTS_URL = 'https://efile.fara.gov/api/v1/Registrants/json/%s'
DOCS_URL = 'https://efile.fara.gov/api/v1/RegDocs/json/%s'
SHORTFORM_URL = 'https://efile.fara.gov/api/v1/ShortFormRegistrants/json/%s/%s'
PRINCIPALS_URL = 'https://efile.fara.gov/api/v1/ForeignPrincipals/json/%s/%s'


def _get_rows(context, res):
    for key, rows in res.json.items():
        if not is_mapping(rows):
            context.log.info("Response [%s]: %s", res.url, rows)
            return
        for row in ensure_list(rows.get('ROW')):
            row['DataTable'] = key
            yield row


def _get_row_id(row):
    items = [v for (k, v) in sorted(row.items()) if k != 'ROWNUM']
    return str(make_id(*items))


def index(context, data):
    table = context.datastore['us_fara_registrants']
    for state in STATES:
        res = context.http.get(REGISTRANTS_URL % state)
        # time.sleep(3)
        for row in _get_rows(context, res):
            row['Status'] = state
            table.upsert(row, ['Registration_Number'])
            context.emit(rule='documents', data=row)
            context.emit(rule='shortform', data=row)
            context.emit(rule='principals', data=row)


def shortform(context, data):
    table = context.datastore['us_fara_shortform']
    reg_nr = data.get('Registration_Number')
    for state in STATES:
        res = context.http.get(SHORTFORM_URL % (state, reg_nr))
        # time.sleep(3)
        for row in _get_rows(context, res):
            row['Status'] = state
            row['RowId'] = _get_row_id(row)
            table.upsert(row, ['RowId'])


def principals(context, data):
    table = context.datastore['us_fara_principals']
    reg_nr = data.get('Registration_Number')
    for state in STATES:
        res = context.http.get(PRINCIPALS_URL % (state, reg_nr))
        # time.sleep(3)
        for row in _get_rows(context, res):
            row['Status'] = state
            row['RowId'] = _get_row_id(row)
            table.upsert(row, ['RowId'])


def documents(context, data):
    reg_nr = data.get('Registration_Number')
    res = context.http.get(DOCS_URL % reg_nr)
    # time.sleep(3)
    for row in _get_rows(context, res):
        row['url'] = row.pop('Url', None)
        context.emit(data=row)


def db_doc(context, data):
    data.pop('headers', None)
    data.pop('request_id', None)
    data.pop('aleph_document', None)
    table = context.datastore['us_fara_documents']
    table.upsert(data, ['url'])
