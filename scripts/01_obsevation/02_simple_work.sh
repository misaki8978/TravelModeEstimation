# 02_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N simple_work
#$ -q all.q@Dwight
#$ -pe smp 1
#$ -o logs/stay_and_move.out
#$ -e logs/stay_and_move.err

PLACE="03_tokyo"
YEAR="2019"

# FILTERED_DIR="$DATA_DIR/interim/filter/${PLACE}/${YEAR}_weekly/"
# FILTERED_DIR="$DATA_DIR/processed/04_01_${PLACE}/${YEAR}_weekly"
FILTERED_DIR="$DATA_DIR/interim/multithread/04_01_${PLACE}_${YEAR}_weekly/basic"
FILTERED_FILES=(${FILTERED_DIR}/assign_stay_ids*.csv)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/01_obsevation/02_stay_and_move.py "${FILTERED_FILES[@]}"



