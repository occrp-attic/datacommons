from urllib.parse import quote
from datetime import datetime
from pprint import pprint  # noqa

FIELDS = "itemid,docname,doctype,application,typedescription,kpdateAsText,documentcollectionid,languageisocode"  # noqa
SEARCH_URL = "http://hudoc.echr.coe.int/app/query/results"
PDF_URL = "http://hudoc.echr.coe.int/app/conversion/docx/pdf?library=ECHR&id=%s&filename=%s.pdf&logEvent=False"  # noqa
LANGUAGES = {"fre": "fr", "eng": "en"}


def emit_result(context, result):
    url = PDF_URL % (
        quote(result["itemid"].encode("utf-8")),
        quote(result["docname"].encode("utf-8")),
    )

    if context.skip_incremental(url):
        return

    data = {
        "file_name": result.get("docname"),
        "foreign_id": result.get("itemid"),
        "url": url,
    }
    lang_code = result.get("languageisocode").lower()
    if lang_code in LANGUAGES:
        data["languages"] = [LANGUAGES.get(lang_code)]
    else:
        context.log.warning("Cannot parse language: %s" % lang_code)

    date_text = result.get("kpdateAsText")
    try:
        if " " in date_text:
            date_text, _ = date_text.split(" ", 1)
        date = datetime.strptime(date_text, "%d/%m/%Y")
        date = date.date().isoformat()
        data["published_at"] = date
    except Exception:
        context.log.warning("Cannot parse date: %s" % date_text)

    context.emit(data=data)


def search(context, data):
    offset = data.get("offset", 0)
    page_size = 100
    context.log.info("ECHR search, offset %s" % offset)
    result = context.http.get(
        SEARCH_URL,
        params={
            "query": "(contentsitename:ECHR)",
            "select": FIELDS,
            "sort": "",
            "start": offset,
            "length": page_size,
        },
    )
    for row in result.json.get("results"):
        emit_result(context, row.get("columns"))

    offset = offset + page_size
    if offset <= result.json.get("resultcount"):
        context.recurse(data={"offset": offset})
