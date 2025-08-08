# 02_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N clustering
#$ -q all.q@Claudette
#$ -pe smp 8
#$ -o logs/clustering.out
#$ -e logs/clustering.err

PLACE="09_nagasaki"
YEAR="2019"

INPUT_DIR="$DATA_DIR/processed/01_03_segment_analysis/${PLACE}/${YEAR}_weekly_weekly"
# FILTERED_DIR="$DATA_DIR/processed/04_01_${PLACE}/${YEAR}_weekly"
INPUT_FILES=(${INPUT_DIR}/*.csv.gz)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/06_clustering/02_clustering.py "${INPUT_FILES[@]}"



