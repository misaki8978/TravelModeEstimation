# 04_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N accuracy_4
#$ -q all.q@Dwight
#$ -pe smp 1
#$ -o logs/010_accuracy/04_trip_compare.out
#$ -e logs/010_accuracy/04_trip_compare.err

PLACE="07_osaka"
YEAR="2019"

GPS_DIR="$DATA_DIR/processed/010_accuracy/${PLACE}/${YEAR}_weekly"
GPS_FILE=(${GPS_DIR}/*by_mode.csv) 

python3 /home/fukui/workspace/TravelModeEstimation/scripts/010_accuracy/04_trip_compare.py "${GPS_FILE[@]}"
