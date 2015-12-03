#!/bin/bash

python scraper.py
python dump.py
python geo.py
python geolayers.py
sh geolayers.sh

ARCHIVE=$DATA_PATH/flexicadastre/archive
mkdir -p $ARCHIVE
OUTFILE=$ARCHIVE/flexiscrape-`date +%Y%m%d`.tgz
echo $OUTFILE
cd $WS/data
tar cfz $OUTFILE $DATA_PATH/flexicadastre/data
