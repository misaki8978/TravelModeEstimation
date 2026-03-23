# 01_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N accuracy
#$ -q all.q@Dwight
#$ -pe smp 1
#$ -o logs/010_accuracy/accuracy.out
#$ -e logs/010_accuracy/accuracy.err

PLACE="09_nagasaki"
YEAR="2019"

FILTER_DIR="$DATA_DIR/processed/06_02/${PLACE}/${YEAR}_weekly/1_non-stops"
FILTER_FILES=(${FILTER_DIR}/*gis_cluster.csv.gz)

FILTERED_DIR="$DATA_DIR/processed/09_04_${PLACE}/${YEAR}_weekly"
FILTERED_FILES=(${FILTERED_DIR}/*isin.csv.gz)

# python3 /home/fukui/workspace/TravelModeEstimation/scripts/07_cluster_filtering/01_filtering.py "${FILTER_FILES[@]}" -- "${FILTERED_FILES[@]}"
python3 /home/fukui/workspace/TravelModeEstimation/scripts/010_accuracy/01_cluster_merge.py "${FILTER_FILES[@]}" "${FILTERED_FILES[@]}"
