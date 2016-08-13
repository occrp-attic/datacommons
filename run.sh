#!/bin/bash

ARCHIVE=$DATA_PATH/flexicadastre/archive
mkdir -p $ARCHIVE
DATA_PATH=$DATA_PATH/current
mkdir -p $DATA_PATH

python scraper.py

for FP in $DATA_PATH/*.geojson; do
    FN=`basename $FP .geojson`
    OP="$DATA_PATH/shapefiles/$FN"
    mkdir -p $OP
    OF="$OP/$FN.shp"
    ogr2ogr -f "ESRI Shapefile" $OF $FP
    echo $OF;
done;

OUTFILE=$ARCHIVE/flexiscrape-`date +%Y%m%d`.tgz
echo $OUTFILE

tar cfz $OUTFILE $DATA_PATH

aws s3 cp $OUTFILE s3://archive.pudo.org/flexicadastre/flexiscrape-`date +%Y%m%d`.tgz
aws s3 cp $OUTFILE s3://archive.pudo.org/flexicadastre/flexiscrape-latest.tgz
