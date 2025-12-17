# 06_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N gis_frag
#$ -q all.q@Cheryl
#$ -pe smp 1
#$ -o logs/01_obsevation/gis_frag.out
#$ -e logs/01_obsevation/gis_frag.err

PLACE="07_osaka"
YEAR="2019"

CLUSTER_DIR="$DATA_DIR/processed/06_02/${PLACE}/${YEAR}_weekly/stops"
# CLUSTER_DIR="$DATA_DIR/processed/06_02_${PLACE}/${YEAR}_weekly"
# FILTERED_DIR="$DATA_DIR/processed/04_01_${PLACE}/${YEAR}_weekly"
CLUSTER_FILES=(${CLUSTER_DIR}/*gis_cluster.csv.gz)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/01_obsevation/06_gis_frag.py "${CLUSTER_FILES[@]}"



