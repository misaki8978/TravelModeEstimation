# 01_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N cluster_finding
#$ -q all.q@Jake
#$ -pe smp 1
#$ -o logs/cluster_finding.out
#$ -e logs/cluster_finding.err

PLACE="09_nagasaki"
YEAR="2019"

FILTERED_DIR="$DATA_DIR/processed/07_01_${PLACE}/${YEAR}_weekly"
FILTERED_FILES=(${FILTERED_DIR}/*.csv.gz)

python /home/fukui/workspace/TravelModeEstimation/scripts/08_cluster_finding/01_cluster_finding.py  ${FILTERED_FILES[@]}