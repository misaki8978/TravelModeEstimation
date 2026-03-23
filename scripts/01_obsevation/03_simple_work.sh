# 03_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N seg_analysis
#$ -q all.q@Cheryl
#$ -pe smp 8
#$ -o logs/seg_analysis.out
#$ -e logs/seg_analysis.err

PLACE="07_osaka"
YEAR="2019"

CLUSTER_DIR="$DATA_DIR/processed/09_04_re/${PLACE}/${YEAR}_weekly/"
# # FILTERED_DIR="$DATA_DIR/processed/04_01_${PLACE}/${YEAR}_weekly"
CLUSTER_FILES=(${CLUSTER_DIR}*nonwalk.csv.gz)

# CLUSTER_DIR="$DATA_DIR/processed/010_accuracy/${PLACE}/${YEAR}_weekly/normal"
# # FILTERED_DIR="$DATA_DIR/processed/04_01_${PLACE}/${YEAR}_weekly"
# CLUSTER_FILES=(${CLUSTER_DIR}*segment.csv.gz)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/01_obsevation/03_segment_analysis.py "${CLUSTER_FILES[@]}"



