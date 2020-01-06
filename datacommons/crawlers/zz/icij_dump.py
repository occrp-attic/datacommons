from normality import slugify, stringify
from csv import DictReader
from zipfile import ZipFile


def load_file(context, zip, name):
    fh = zip.open(name)
    _, section, _ = name.rsplit('.', 2)
    table_name = '%s_%s' % (context.crawler.name, section)
    table = context.datastore[table_name]
    table.drop()
    reader = DictReader(fh, delimiter=',', quotechar='"')
    chunk = []
    for i, row in enumerate(reader, 1):
        row = {slugify(k, sep='_'): stringify(v) for (k, v) in row.items()}
        chunk.append(row)
        if len(chunk) >= 20000:
            context.log.info("Loaded [%s]: %s rows...", table_name, i)
            table.insert_many(chunk)
            chunk = []
    if len(chunk):
        table.insert_many(chunk)
    context.log.info("Done [%s]: %s rows...", table_name, i)


def load(context, data):
    with context.http.rehash(data) as result:
        with ZipFile(result.file_path, 'r') as zip:
            for name in zip.namelist():
                load_file(context, zip, name)
