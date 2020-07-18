import io
from csv import DictReader
from zipfile import ZipFile
from normality import slugify, stringify
from dataset.chunked import ChunkedInsert


def load_file(context, zip, name):
    fh = zip.open(name)
    _, section, _ = name.rsplit(".", 2)
    table_name = "%s_%s" % (context.crawler.name, section)
    table = context.datastore[table_name]
    table.drop()
    fh = io.TextIOWrapper(fh)
    reader = DictReader(fh, delimiter=",", quotechar='"')
    chunk = ChunkedInsert(table)
    for i, row in enumerate(reader, 1):
        row = {slugify(k, sep="_"): stringify(v) for (k, v) in row.items()}
        chunk.insert(row)
    chunk.flush()
    context.log.info("Done [%s]: %s rows...", table_name, i)


def load(context, data):
    with context.http.rehash(data) as result:
        with ZipFile(result.file_path, "r") as zip:
            for name in zip.namelist():
                load_file(context, zip, name)
