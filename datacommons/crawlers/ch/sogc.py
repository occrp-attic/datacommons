from urllib.parse import urljoin
from normality import collapse_spaces

TYPES = ["BL", "KK", "SR", "HR", "UP", "NA", "SB", "IS", "AW", "IP", "AN", "VM", "AB"]
CANTONS = [
    "AG",
    "AI",
    "AR",
    "BE",
    "BL",
    "BS",
    "FR",
    "GE",
    "GL",
    "GR",
    "JU",
    "LU",
    "NE",
    "NW",
    "OW",
    "SG",
    "SH",
    "SO",
    "SZ",
    "TG",
    "TI",
    "UR",
    "VD",
    "VS",
    "ZG",
    "ZH",
]
TYPES_CANTON = ["AB", "HR", "KK", "NA", "SB"]
QUERY = {
    "SELTYPE": "",
    "SEARCH_SAVE": "",
    "KEYWORDS": "",
    "ORGANISATION_TX": "",
    "NOTICE_NR": "",
    "TIMESPAN": "VARIABLE",
    "command": "Search",
}


def emit_document(context, row, date):
    context.http.reset()
    url = context.params.get("url")
    cells = row.findall("./td")
    if not len(cells):
        return

    text = [c.text_content().strip() for c in cells]
    _, num, category, name, _ = text
    title = "%s (%s, %s)" % (name, category, date)
    title = collapse_spaces(title)

    link = row.find('.//a[@class="pdfLnk"]')
    if link is None:
        return
    url = urljoin(url, link.get("href"))

    context.emit(
        data={
            "url": url,
            "title": title,
            "foreign_id": url,
            "countries": ["ch"],
            "dates": [date],
            "extension": "pdf",
            "mime_type": "application/pdf",
        }
    )


def search(context, data):
    meta_date = data.get("date_iso")[:10]
    context.http.reset()
    url = context.params.get("url")
    result = context.http.post(url, data=data.get("query"))
    page = 1
    while True:
        rows = result.html.findall('.//table[@id="resultList"]//tr')
        if len(rows) < 2:
            break
        for row in rows:
            emit_document(context, row, meta_date)
        page += 1
        page_url = url + "?EID=1&PAGE=%s" % page
        result = context.http.get(page_url)


def args(context, data):
    date = data.get("date")
    if context.skip_incremental(date):
        return

    context.log.info("SOGC search: %s", date)

    for seltype in TYPES:
        query = QUERY.copy()
        query["STAT_TM_1"] = date
        query["STAT_TM_2"] = date
        query["SELTYPE"] = seltype

        if seltype in TYPES_CANTON:
            for canton in CANTONS:
                subquery = query.copy()
                field = "%s_CANTON_%s" % (seltype, canton)
                subquery[field] = "ON"
                data["query"] = subquery
                context.emit(data=data)
        else:
            data["query"] = query
            context.emit(data=data)
