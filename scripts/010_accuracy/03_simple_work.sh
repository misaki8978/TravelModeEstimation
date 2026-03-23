# 03_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N accuracy_2
#$ -q all.q@Dwight
#$ -pe smp 1
#$ -o logs/010_accuracy/03_cluster_sample.out
#$ -e logs/010_accuracy/03_cluster_sample.err

PLACE="07_osaka"
YEAR="2019"

GPS_DIR="$DATA_DIR/processed/010_accuracy/${PLACE}/${YEAR}_weekly"
GPS_FILE=(${GPS_DIR}/*mode_segment.csv.gz) 
TRUE_FILE="$DATA_DIR/processed/010_accuracy/${PLACE}/${YEAR}_weekly/segment_labeling_sheet.csv"

python3 /home/fukui/workspace/TravelModeEstimation/scripts/010_accuracy/03_accuracy.py "${GPS_FILE[@]}" "${TRUE_FILE}"
