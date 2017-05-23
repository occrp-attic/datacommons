# flexicadastre

The ``scraper.py`` scripts is an importer for [FlexiCadastre](http://www.spatialdimension.com/Map-Portals), a GIS solution developed by SpatialDimension and used to store extractives licensing info for a variety of countries.

This scraper is run automatically on ``morph.io``. You can find the runtime status and the scraped data at: [https://morph.io/pudo/flexicadastre](https://morph.io/pudo/flexicadastre).

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

## License

The MIT License (MIT)

Copyright (c) 2015-2017 Friedrich Lindenberg

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
