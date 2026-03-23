# 05_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N cluster_finding
#$ -q all.q@Dwight
#$ -pe smp 1
#$ -o logs/01_obsevation/cluster_finding.out
#$ -e logs/01_obsevation/cluster_finding.err

PLACE="07_osaka"
YEAR="2019"

CLUSTER_DIR="$DATA_DIR/processed/010_accuracy/${PLACE}/${YEAR}_weekly/normal"
# FILTERED_DIR="$DATA_DIR/processed/04_01_${PLACE}/${YEAR}_weekly"
CLUSTER_FILES=(${CLUSTER_DIR}*gps.csv.gz)

GIS_DIR="$DATA_DIR/interim/GIS_geojson/route"
GIS_FILE=(${GIS_DIR}/${PLACE}*.shp)
TRAIN_FILE=(${GIS_DIR}/japan*.shp)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/01_obsevation/05_cluster_finding.py "${CLUSTER_FILES[@]}" -- "${GIS_FILE[@]}" "${TRAIN_FILE[@]}"



