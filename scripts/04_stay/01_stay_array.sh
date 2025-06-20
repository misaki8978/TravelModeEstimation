# 01_array_template.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N gps_weekly
#$ -q all.q@Cheryl
#$ -pe smp 8
#$ -t 1-12
#$ -o logs/09_nagasaki_2019/$TASK_ID.out
#$ -e logs/09_nagasaki_2019/$TASK_ID.err

export OMP_NUM_THREADS=8

# Split 09_nagasaki_2019 into place and year
PLACE_YEAR="09_nagasaki_2019"
PLACE=$(echo "$PLACE_YEAR" | sed 's/_[^_]*$//')  # Gets everything before the last underscore
YEAR=$(echo "$PLACE_YEAR" | sed 's/.*_//')       # Gets everything after the last underscore

echo "Using place: $PLACE" >&1
echo "Using year: $YEAR" >&1
echo "Using place_year: $PLACE_YEAR" >&1

CHUNK_DIR="$DATA_DIR/interim/chunks/${PLACE}_${YEAR}_gps"
# FILTER_DIR="$DATA_DIR/interim/filter/${PLACE}/${YEAR}_weekly"

echo "CHUNK: $CHUNK_DIR"

CHUNK_FILE=$(printf "%s/chunk_%02d" "$CHUNK_DIR" $((SGE_TASK_ID-1)))
# FILTER_FILES=(${FILTER_DIR}/*.csv.gz)

# ファイル名の配列に読み込んで Python に渡す
mapfile -t FILES < "$CHUNK_FILE"
python3 /home/fukui/workspace/TravelModeEstimation/scripts/04_stay/01_stay_detection.py "${FILES[@]}"
