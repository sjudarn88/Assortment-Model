#!/bin/bash

str=$1
div=$2
soar=$3
season=$4

/home/dan_shao/anaconda3/bin/python apparel_AO_v2_0430_Scripts_Optimum_Assortment.py $str $div $soar $season

echo Moving to GCP..
gsutil cp ./$str'_'$soar'_'$div'_'$season/AO_output_file_$str'_'$soar'_'$div'_'$season.csv gs://ao-v2-bucket/output/AO_output_file_dannie_$str'_'$soar'_'$div'_'$season.csv 

echo Loading to BQ..
bq load --autodetect --noreplace --source_format=CSV apparel_ao_v2_allstores.ao_output_v2_0810_dannie_$season gs://ao-v2-bucket/output/AO_output_file_dannie_$str'_'$soar'_'$div'_'$season.csv

exit 0
