# 02_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N simple_work
#$ -q all.q@Claudette
#$ -pe smp 1
#$ -o logs/02_filtering/simple_work.out
#$ -e logs/02_filtering/simple_work.err

PLACE="08_fukuoka"
YEAR="2022"

FILTERED_DIR="$DATA_DIR/interim/filtered/${PLACE}/${YEAR}_weekly/user_counts_4500/bulk/"

FILTERED_FILES=(${FILTERED_DIR}/*.csv.gz)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/02_filtering/02_data_sort.py "${FILTERED_FILES[@]}"



