# 03_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N plot_all
#$ -q all.q@Dwight
#$ -pe smp 8
#$ -o logs/plot_all.out
#$ -e logs/plot_all.err

PLACE="09_nagasaki"
YEAR="2019"

# INPUT_DIR="$DATA_DIR/processed/05_01_${PLACE}_replay/${YEAR}_weekly"
INPUT_DIR="$DATA_DIR/processed/09_04_${PLACE}/${YEAR}_weekly"

GIS_DIR="$DATA_DIR/interim/GIS_geojson"
# FILTERED_DIR="$DATA_DIR/processed/04_01_${PLACE}/${YEAR}_weekly"
INPUT_FILES=(${INPUT_DIR}/*isin.csv.gz)
GIS_FILE=(${GIS_DIR}/nagasaki*.geojson)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/09_gis/03_plot_all.py "${INPUT_FILES[@]}" -- "${GIS_FILE[@]}"



