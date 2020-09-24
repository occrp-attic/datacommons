# import time
import json
from normality import stringify
from banal import ensure_list, is_mapping
from memorious.helpers.key import make_id
from urllib.parse import urlparse

# https://www.fara.gov/search.html
# https://efile.fara.gov/ords/f?p=107:3:0::NO:::
STATES = ("Active", "Terminated")
REGISTRANTS_URL = "https://efile.fara.gov/api/v1/Registrants/json/%s"
DOCS_URL = "https://efile.fara.gov/api/v1/RegDocs/json/%s"
SHORTFORM_URL = "https://efile.fara.gov/api/v1/ShortFormRegistrants/json/%s/%s"
PRINCIPALS_URL = "https://efile.fara.gov/api/v1/ForeignPrincipals/json/%s/%s"


def _get_row_data(row):
    data = {}
    for key, value in row.items():
        key = stringify(key)
        if key is None:
            continue
        data[key] = stringify(value)
    return data


def _get_rows(context, res):
    try:
        data = res.json
    except json.JSONDecodeError:
        context.emit_warning("Request error: %s" % res.text)
        return
    for key, rows in data.items():
        if not is_mapping(rows):
            context.log.info("Response [%s]: %s", res.url, rows)
            return
        for row in ensure_list(rows.get("ROW")):
            row = _get_row_data(row)
            row["DataTable"] = stringify(key)
            yield row


def _get_row_id(row):
    items = [v for (k, v) in sorted(row.items()) if k != "ROWNUM"]
    return str(make_id(*items))


def _get_filename(row):
    url = row.get('url', None)
    if url is not None:
        return '.'.join(url.split('/')[-1].split('.')[:-1])
    else:
        return None


def index(context, data):
    table = context.datastore["us_fara_registrants"]
    for state in STATES:
        res = context.http.get(REGISTRANTS_URL % state)
        for row in _get_rows(context, res):
            row["Status"] = state
            table.upsert(row, ["Registration_Number"])
            context.emit(rule="documents", data=row)
            context.emit(rule="shortform", data=row)
            context.emit(rule="principals", data=row)


def shortform(context, data):
    table = context.datastore["us_fara_shortform"]
    reg_nr = data.get("Registration_Number")
    for state in STATES:
        res = context.http.get(SHORTFORM_URL % (state, reg_nr))
        for row in _get_rows(context, res):
            row["Status"] = state
            row["RowId"] = _get_row_id(row)
            table.upsert(row, ["RowId"])


def principals(context, data):
    table = context.datastore["us_fara_principals"]
    reg_nr = data.get("Registration_Number")
    for state in STATES:
        res = context.http.get(PRINCIPALS_URL % (state, reg_nr))
        for row in _get_rows(context, res):
            row["Status"] = state
            row["RowId"] = _get_row_id(row)
            table.upsert(row, ["RowId"])


def documents(context, data):
    reg_nr = data.get("Registration_Number")
    res = context.http.get(DOCS_URL % reg_nr)
    for row in _get_rows(context, res):
        row["url"] = row.pop("Url", None)
        parsedurl = urlparse(row["url"])
        if parsedurl.scheme and parsedurl.netloc:
            row["file_name"] = _get_filename(row)
            context.emit(data=row)


def db_doc(context, data):
    data.pop("headers", None)
    data.pop("request_id", None)
    data.pop("aleph_document", None)
    table = context.datastore["us_fara_documents"]
    table.upsert(data, ["url"])
