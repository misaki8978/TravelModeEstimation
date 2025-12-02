# 05_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N cluster_finding
#$ -q all.q@Dwight
#$ -pe smp 1
#$ -o logs/cluster_finding.out
#$ -e logs/cluster_finding.err

PLACE="09_nagasaki"
YEAR="2019"

CLUSTER_DIR="$DATA_DIR/processed/07_01/${PLACE}/${YEAR}_weekly"
# FILTERED_DIR="$DATA_DIR/processed/04_01_${PLACE}/${YEAR}_weekly"
CLUSTER_FILES=(${CLUSTER_DIR}/*cluster_data.csv.gz)

GIS_DIR="$DATA_DIR/interim/GIS_geojson"
GIS_FILE=(${GIS_DIR}/09_nagasaki*.shp)
TRAIN_FILE=(${GIS_DIR}/japan*.shp)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/01_obsevation/05_cluster_finding.py "${CLUSTER_FILES[@]}" -- "${GIS_FILE[@]}" "${TRAIN_FILE[@]}"



