# 06_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N gis_frag
#$ -q all.q@Dwight
#$ -pe smp 1
#$ -o logs/gis_frag.out
#$ -e logs/gis_frag.err

PLACE="09_nagasaki"
YEAR="2019"

CLUSTER_DIR="$DATA_DIR/processed/06_02_${PLACE}/${YEAR}_weekly"
# FILTERED_DIR="$DATA_DIR/processed/04_01_${PLACE}/${YEAR}_weekly"
CLUSTER_FILES=(${CLUSTER_DIR}/seg_fuzzy_cluster.csv.gz)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/01_obsevation/06_gis_frag.py "${CLUSTER_FILES[@]}"



