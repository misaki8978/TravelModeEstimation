# 01_plus_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N simple_work
#$ -q all.q@Dwight
#$ -pe smp 1
#$ -o logs/simple_work.out
#$ -e logs/simple_work.err

PLACE="09_nagasaki"
YEAR="2019"

FILTERED_DIR="$DATA_DIR/interim/agg_before_filter/${PLACE}_${YEAR}/merged"
# FILTERED_DIR="$DATA_DIR/processed/04_01_${PLACE}/${YEAR}_weekly"
FILTERED_FILES=(${FILTERED_DIR}/*.csv.gz)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/01_obsevation/01_plus_observation.py "${FILTERED_FILES[@]}"



