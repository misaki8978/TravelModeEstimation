# 04_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N time_diff
#$ -q all.q@Dwight
#$ -pe smp 1
#$ -o logs/time_diff.out
#$ -e logs/time_diff.err

PLACE="09_nagasaki"
YEAR="2019"

FILTERED_DIR="$DATA_DIR/interim/filtered/${PLACE}/${YEAR}_weekly/user_counts_4500/sorted"
# FILTERED_DIR="$DATA_DIR/processed/04_01_${PLACE}/${YEAR}_weekly"
FILTERED_FILES=(${FILTERED_DIR}/*.csv.gz)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/01_obsevation/04_time_diff.py "${FILTERED_FILES[@]}"



