# 05_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N accuracy_5
#$ -q all.q@Dwight
#$ -pe smp 1
#$ -o logs/010_accuracy/05_cluster_yer.out
#$ -e logs/010_accuracy/05_cluster_yer.err

PLACE="07_osaka"
# YEAR="2019"
VERSION="2_stops"

GPS_DIR="$DATA_DIR/processed/010_accuracy/${PLACE}"
GPS_FILE_2019=(${GPS_DIR}/2019_weekly/${VERSION}*by_mode.csv)
# GPS_FILE_2020=(${GPS_DIR}/2020_weekly/${VERSION}*by_mode.csv)
# GPS_FILE_2021=(${GPS_DIR}/2021_weekly/${VERSION}*by_mode.csv)
# GPS_FILE_2022=(${GPS_DIR}/2022_weekly/${VERSION}*by_mode.csv)

# python3 /home/fukui/workspace/TravelModeEstimation/scripts/010_accuracy/05_mode_raito_byyear.py "${GPS_FILE_2019[@]}" "${GPS_FILE_2020[@]}" "${GPS_FILE_2021[@]}" "${GPS_FILE_2022[@]}"
python3 /home/fukui/workspace/TravelModeEstimation/scripts/010_accuracy/05_mode_raito_byyear.py "${GPS_FILE_2019[@]}"
