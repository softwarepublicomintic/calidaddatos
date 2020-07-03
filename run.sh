#!/bin/bash

#Settings

source parameters.config

if [ $? == 1 ]
then
    echo "There was an incidentes loading configuration file"
else
    python3 ./application.py

fi
