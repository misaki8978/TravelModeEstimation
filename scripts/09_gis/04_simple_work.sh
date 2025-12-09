# 04_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N gis_features
#$ -q all.q@Claudette
#$ -pe smp 8
#$ -o logs/09_gis/gis_features.out
#$ -e logs/09_gis/gis_features.err

PLACE="09_nagasaki"
YEAR="2019"

INPUT_DIR="$DATA_DIR/processed/05_01_re/${PLACE}/${YEAR}_weekly"
GIS_DIR="$DATA_DIR/interim/GIS_geojson"

INPUT_FILES=(${INPUT_DIR}/*.csv.gz)
GIS_FILES=(${GIS_DIR}/${PLACE}*.shp)
RAIL_FILES=(${GIS_DIR}/japan*.shp)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/09_gis/04_gis_features.py "${INPUT_FILES[@]}" -- "${GIS_FILES[@]}" "${RAIL_FILES[@]}"

# FILTERED_DIR="$DATA_DIR/processed/09_04_re/${PLACE}/${YEAR}_weekly"
# INPUT_FILES=(${FILTERED_DIR}/*isin.csv.gz)
# WALK_FILES=(${FILTERED_DIR}/walk*.csv.gz)

# python3 /home/fukui/workspace/TravelModeEstimation/scripts/09_gis/04_gis_features.py "${INPUT_FILES[@]}" "${WALK_FILES[@]}"


