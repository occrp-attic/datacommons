FROM debian:jessie

RUN apt-get update \
    && apt-get install -y \
        python-pip python-dev libxml2-dev libxslt1-dev build-essential lib32z1-dev
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

RUN mkdir /scraper
COPY scraper.py /scraper
WORKDIR /scraper
CMD python scraper.py
