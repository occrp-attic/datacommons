from pprint import pprint  # noqa
from datetime import datetime
from normality import stringify

# from memorious.helpers import convert_snakecase

REMOVE = ['Shape.STArea()', 'Shape.STLength()', 'FullShapeGeometryWKT']

def convert_data(data):
    # this converts all values in the attribute data to a
    # form suitable for the database storage.
    row = {}
    for name, val in data.items():
        uname = name.upper()
        if val is not None and isinstance(val, int):
            if uname.startswith('DTE') or uname.endswith('_DAT') \
                    or uname.endswith('_DATE') or uname.endswith('_D') \
                    or uname == 'COMPLETED':
                dt = datetime.fromtimestamp(int(val) / 1000)
                val = dt.date().isoformat()
        if uname.startswith('GUID'):
            continue
        if name in REMOVE:
            continue
        if uname == 'AREA':
            val = min(val, (2 ** 31) - 1)
        val = stringify(val)
        row[name] = val
    return row
