# 01_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N simple_work
#$ -q all.q@Claudette
#$ -pe smp 1
#$ -o logs/simple_work.out
#$ -e logs/simple_work.err

PLACE="09_nagasaki"
YEAR="2019"

# FILTERED_DIR="$DATA_DIR/interim/filter/${PLACE}/${YEAR}_weekly/"
# DIR_segmented="$DATA_DIR/processed/05_01_${PLACE}/${YEAR}_weekly/"
# FILTERED_DIR="$DATA_DIR/processed/04_01_${PLACE}/${YEAR}_weekly"
filter_dir="$DATA_DIR/interim/filter/${PLACE}/${YEAR}_weekly"
FILTERED_FILES=(${filter_dir}/*.csv.gz)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/00_template/file_info.py "${FILTERED_FILES[@]}"



