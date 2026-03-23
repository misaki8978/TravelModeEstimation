# 04_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N gis_features
#$ -q all.q@Dwight
#$ -pe smp 8
#$ -o logs/09_gis/gis_features.out
#$ -e logs/09_gis/gis_features.err

PLACE="09_nagasaki"
YEAR="2019"

# INPUT_DIR="$DATA_DIR/processed/05_01_re/${PLACE}/${YEAR}_weekly"
INPUT_DIR="$DATA_DIR/processed/09_04_${PLACE}/${YEAR}_weekly"
ROUTE_DIR="$DATA_DIR/interim/GIS_geojson/route"
STOP_DIR="$DATA_DIR/interim/GIS_geojson/stop"

INPUT_FILES=(${INPUT_DIR}/*.csv.gz)
BUS_FILES=(${ROUTE_DIR}/${PLACE}*.shp)
RAIL_FILES=(${ROUTE_DIR}/japan*.shp)
BUS_STOP_FILES=(${STOP_DIR}/${PLACE}*.shp)
TRAIN_STOP_FILES=(${STOP_DIR}/japan*.shp)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/09_gis/04_gis_features.py "${INPUT_FILES[@]}" -- "${BUS_FILES[@]}" "${RAIL_FILES[@]}" "${BUS_STOP_FILES[@]}" "${TRAIN_STOP_FILES[@]}"

# FILTERED_DIR="$DATA_DIR/processed/09_04_re/${PLACE}/${YEAR}_weekly"
# INPUT_FILES=(${FILTERED_DIR}/*isin.csv.gz)
# WALK_FILES=(${FILTERED_DIR}/walk*.csv.gz)

# python3 /home/fukui/workspace/TravelModeEstimation/scripts/09_gis/04_gis_features.py "${INPUT_FILES[@]}" "${WALK_FILES[@]}"


