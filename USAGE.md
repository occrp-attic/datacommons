## Usage

Please set up either Python ``virtualenv`` or Docker on your computer to isolate this project. If you're using ``virtualenv``, the
following should get you up and running:

~~~~ bash
$ git clone https://github.com/pudo/flexicadastre.git flexicadastre
$ cd flexicadastre
# Set up the virtualenv (you can also use mkvirtualenv if you have it installed)
$ virtualenv env
# Activate the virtual environment:
$ source env/bin/activate
# Install the dependencies
(env)$ pip install -r requirements.txt
# OPTIONAL: set a permanent DB location
(env)$ export DATABASE_URI="sqlite:///flexicadastre.sqlite"
# Run the actual scraper:
(env)$ python scraper.py
~~~~

The generated GeoJSON and CSV files will be stored in ``data/`` by default, you can set an alternate location using the ``DATA_PATH`` environment variable.
