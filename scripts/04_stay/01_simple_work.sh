# 01_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N simple_work
#$ -q all.q@Claudette
#$ -pe smp 1
#$ -o logs/01_simple_work.out
#$ -e logs/01_simple_work.err

PLACE="09_nagasaki"
YEAR="2019"

FILTERED_DIR="$DATA_DIR/interim/filtered/${PLACE}/${YEAR}_weekly/user_counts_4500/sorted/"

# FILTERED_FILES=(${FILTERED_DIR}/*.csv.gz)
FILTERED_FILES=("${FILTERED_DIR}/2019-02_sorted_gps_data.csv.gz")

# python3 /home/fukui/workspace/TravelModeEstimation/scripts/04_stay/01_stay_detection.py "${FILTERED_FILES[@]}"

python3 /home/fukui/workspace/TravelModeEstimation/scripts/04_stay/01_stay_detection.py "${FILTERED_DIR}/2019-02_sorted_gps_data.csv.gz"


