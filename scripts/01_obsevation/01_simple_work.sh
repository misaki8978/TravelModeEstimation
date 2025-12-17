# 01_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N simple_work
#$ -q all.q@Dwight
#$ -pe smp 1
#$ -o logs/simple_work.out
#$ -e logs/simple_work.err

PLACE="03_tokyo"
YEAR="2019"

# FILTERED_DIR="$DATA_DIR/interim/filter/${PLACE}/${YEAR}_weekly/"
FILTERED_DIR="$DATA_DIR/processed/09_04_re/${PLACE}/${YEAR}_weekly"
FILTERED_FILES=(${FILTERED_DIR}/*nonwalk.csv.gz)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/01_obsevation/01_weekly_data.py "${FILTERED_FILES[@]}"



