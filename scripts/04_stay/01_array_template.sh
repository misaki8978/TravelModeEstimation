# 01_array_template.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N stay_detection
#$ -q all.q@Dwight
#$ -pe smp 10
#$ -t 1-__NUM__
#$ -tc 3
#$ -o logs/04_stay/__PLACE_YEAR__/$TASK_ID.out
#$ -e logs/04_stay/__PLACE_YEAR__/$TASK_ID.err

export OMP_NUM_THREADS=8
echo "Using place_year: __PLACE_YEAR__" >&1
# Split __PLACE_YEAR__ into place and year
PLACE_YEAR="__PLACE_YEAR__"
PLACE=$(echo "$PLACE_YEAR" | sed 's/_[^_]*$//')  # Gets everything before the last underscore
YEAR=$(echo "$PLACE_YEAR" | sed 's/.*_//')       # Gets everything after the last underscore

echo "Using place: $PLACE" >&1
echo "Using year: $YEAR" >&1
echo "Using place_year: $PLACE_YEAR" >&1

CHUNK_DIR="$DATA_DIR/interim/chunks/${PLACE}_${YEAR}_gps"
# FILTER_DIR="$DATA_DIR/interim/filter/${PLACE}/${YEAR}_weekly"

echo "CHUNK: $CHUNK_DIR"
# 月ごとチャンクの一覧を配列に展開
# 例: chunk_2019-01.lst, chunk_2019-02.lst, ...
CHUNK_FILES=( "$CHUNK_DIR"/chunk_*.lst )

# SGE_TASK_ID は 1 始まりなので -1 して 0 始まりのインデックスに
CHUNK_FILE="${CHUNK_FILES[$((SGE_TASK_ID-1))]}"

# ファイル名の配列に読み込んで Python に渡す
mapfile -t FILES < "$CHUNK_FILE"
python3 /home/fukui/workspace/TravelModeEstimation/scripts/04_stay/01_stay_detection.py "${FILES[@]}" "$((SGE_TASK_ID-1))"
