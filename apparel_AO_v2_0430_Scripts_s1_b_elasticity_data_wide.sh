#!/bin/bash

rfile=$1
str=$2
div=$3
soar=$4
season=$5
R --vanilla --slave --args $* < $rfile
exit 0
