import os
import logging
import dataset
import requests.packages.urllib3


DATABASE_URI = os.environ.get('DATABASE_URI', 'sqlite:///:memory:')
assert DATABASE_URI is not None

DATA_PATH = os.environ.get('DATA_PATH', 'data')
assert DATA_PATH is not None

database = dataset.connect(DATABASE_URI)

requests.packages.urllib3.disable_warnings()
logging.basicConfig(level=logging.INFO)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('alembic').setLevel(logging.WARNING)
