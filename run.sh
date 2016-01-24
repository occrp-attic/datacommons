#!/bin/bash

ARCHIVE=$DATA_PATH/flexicadastre/archive
mkdir -p $ARCHIVE

python scraper.py
python parse.py
python notify.py
python dump.py
python geo.py
python geolayers.py

for FP in $DATA_PATH/flexicadastre/data/geo_layers/*.json; do
    FN=`basename $FP .json`
    OP="$DATA_PATH/flexicadastre/data/shapefiles/$FN"
    mkdir -p $OP
    OF="$OP/$FN.shp"
    ogr2ogr -f "ESRI Shapefile" $OF $FP
    echo $OF;
done;

OUTFILE=$ARCHIVE/flexiscrape-`date +%Y%m%d`.tgz
echo $OUTFILE

cd $DATA_PATH/flexicadastre
tar cfz $OUTFILE data

aws s3 cp $OUTFILE s3://archive.pudo.org/flexicadastre/flexiscrape-`date +%Y%m%d`.tgz
aws s3 cp $OUTFILE s3://archive.pudo.org/flexicadastre/flexiscrape-latest.tgz
