# 05_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N cluster_finding
#$ -q all.q@Claudette
#$ -pe smp 1
#$ -o logs/cluster_finding.out
#$ -e logs/cluster_finding.err

PLACE="09_nagasaki"
YEAR="2019"

CLUSTER_DIR="$DATA_DIR/processed/06_02_${PLACE}/${YEAR}_weekly"
# FILTERED_DIR="$DATA_DIR/processed/04_01_${PLACE}/${YEAR}_weekly"
CLUSTER_FILES=(${CLUSTER_DIR}/seg_cluster_fuzzy.csv.gz)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/01_obsevation/05_cluster_finding.py "${CLUSTER_FILES[@]}"



