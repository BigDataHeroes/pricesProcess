#!/bin/bash

source activate keepcodingFinalProject
source properties.sh

python price_evolution.py $inputF $outputF
