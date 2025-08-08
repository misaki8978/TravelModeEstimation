# 06_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N make_features
#$ -q all.q@Claudette
#$ -pe smp 8
#$ -o logs/make_features.out
#$ -e logs/make_features.err

PLACE="09_nagasaki"
YEAR="2019"

INPUT_DIR="$DATA_DIR/processed/05_01_${PLACE}/${YEAR}_weekly"
# FILTERED_DIR="$DATA_DIR/processed/04_01_${PLACE}/${YEAR}_weekly"
INPUT_FILES=(${INPUT_DIR}/*.csv.gz)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/06_clustering/01_make_features.py "${INPUT_FILES[@]}"



