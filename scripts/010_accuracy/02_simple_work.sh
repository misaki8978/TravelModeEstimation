# 02_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N cluster_sample
#$ -q all.q@Dwight
#$ -pe smp 1
#$ -o logs/010_accuracy/02_cluster_sample.out
#$ -e logs/010_accuracy/02_cluster_sample.err

PLACE="07_osaka"
YEAR="2019"

GPS_DIR="$DATA_DIR/processed/010_accuracy/${PLACE}/${YEAR}_weekly"
GPS_FILE=(${GPS_DIR}/1_non*mode_gps.csv.gz) 


python3 /home/fukui/workspace/TravelModeEstimation/scripts/010_accuracy/02_cluster_sample.py "${GPS_FILE[@]}"
