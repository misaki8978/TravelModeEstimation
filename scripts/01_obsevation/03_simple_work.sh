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

PLACE="09_nagasaki"
YEAR="2019"

# INPUT_DIR="$DATA_DIR/processed/05_01_${PLACE}_replay/${YEAR}_weekly_basic"
FILTERED_DIR="$DATA_DIR/processed/05_01_re/${PLACE}/${YEAR}_weekly"
INPUT_FILES=(${FILTERED_DIR}/*.csv.gz)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/01_obsevation/03_segment_analysis.py "${INPUT_FILES[@]}"



