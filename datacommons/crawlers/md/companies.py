# coding: utf-8
from normality import stringify
from collections import OrderedDict

# from tempfile import NamedTemporaryFile
from openpyxl import load_workbook


URL = "http://date.gov.md/ckan/en/dataset/11736-date-din-registrul-de-stat-al-unitatilor-de-drept-privind-intreprinderile-inregistrate-in-repu"  # noqa


def sheet_rows(book, name):
    sheet = book[name]
    headers = None
    for row in sheet.rows:
        row = [c.value for c in row]
        if headers is None:
            headers = []
            for idx, header in enumerate(row):
                header = stringify(header)
                if not isinstance(header, str):
                    header = "column_%s" % idx
                if "(" in header:
                    header, _ = header.split("(")
                if "/" in header:
                    header, _ = header.split("/")
                header = header.replace(" ", "_")
                header = header.replace(".", "_")
                header = header.strip("_")
                headers.append(header)
            continue
        # print(OrderedDict(zip(headers, row)))
        yield OrderedDict(zip(headers, row))


def subfield(row, field):
    value = row.pop(field, None)
    if value is None:
        return
    if isinstance(value, (int, float)):
        yield str(value)
        return
    for item in value.split(", "):
        item = item.strip()
        if len(item):
            yield item


def catalog(context, data):
    with context.http.rehash(data) as result:
        data_url = None
        for res in result.html.findall('.//li[@class="resource-item"]'):
            for link in res.findall(".//a"):
                link = link.get("href")
                if link and link.lower().endswith(".xlsx"):
                    data_url = link

        context.log.info("Data catalog link: %s" % data_url)
        context.emit(data={"url": data_url})


def parse(context, data):
    with context.http.rehash(data) as result:
        with open(result.file_path, "rb") as fh:
            book = load_workbook(fh, read_only=True, data_only=True, guess_types=False)
            unlicensed = {}
            for row in sheet_rows(book, "Clasificare nelicentiate"):
                unlicensed[str(row.get("ID"))] = row

            licensed = {}
            for row in sheet_rows(book, "Clasificare licentiate"):
                licensed[str(row.get("ID"))] = row

            for index, row in enumerate(sheet_rows(book, "RSUD"), 1):
                parse_company(context, index, row, unlicensed, licensed)


def parse_company(context, index, row, unlicensed, licensed):
    row["id"] = index
    idno = row.get(u"IDNO")
    name = row.get(u"Denumirea_completă")
    if name is None:
        return
    date = row.pop(u"Data_înregistrării")
    if date is not None:
        row[u"Data_înregistrării"] = date.date().isoformat()

    base = {"Company_IDNO": idno, "Company_ID": index, "Company_Name": name}
    context.log.info("Company: %s", base.get("Company_Name"))

    for item in subfield(row, "Genuri_de_activitate_nelicentiate"):
        ctx = unlicensed.get(item)
        if ctx is None:
            continue
        ctx = dict(ctx)
        ctx.update(base)
        context.emit(rule="unlicensed", data=ctx)

    for item in subfield(row, "Genuri_de_activitate_licentiate"):
        ctx = licensed.get(item)
        if ctx is None:
            continue
        ctx = dict(ctx)
        ctx.update(base)
        context.emit(rule="licensed", data=ctx)

    for item in subfield(row, "Lista_fondatorilor"):
        data = base.copy()
        data["Founder"] = item
        context.emit(rule="founder", data=data)

    for item in subfield(row, u"Lista_conducătorilor"):
        data = base.copy()
        data["Director"] = item
        context.emit(rule="director", data=data)

    context.emit(rule="company", data=row)
