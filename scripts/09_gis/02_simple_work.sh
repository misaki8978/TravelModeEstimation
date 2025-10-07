# 02_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N transportation
#$ -q all.q@Dwight
#$ -pe smp 8
#$ -o logs/transportation.out
#$ -e logs/transportation.err

PLACE="09_nagasaki"
YEAR="2019"

# INPUT_DIR="$DATA_DIR/processed/05_01_${PLACE}_replay/${YEAR}_weekly_density"
RESULT_DIR="$DATA_DIR/processed/09_01_${PLACE}/${YEAR}_weekly"
FILTERED_DIR="$DATA_DIR/processed/05_01_${PLACE}/${YEAR}_weekly"
INPUT_FILES=(${FILTERED_DIR}/*.csv.gz)
RESULT_FILES=(${RESULT_DIR}/*.csv)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/09_gis/02_make_frag.py "${INPUT_FILES[@]}" -- "${RESULT_FILES[@]}"



