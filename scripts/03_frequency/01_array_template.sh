# 01_array_template.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N gps_weekly
#$ -q all.q@Cheryl
#$ -pe smp 1
#$ -t 1-__NUM__
#$ -o logs/__PLACE_YEAR__/$TASK_ID.out
#$ -e logs/__PLACE_YEAR__/$TASK_ID.err


echo "Using place_year: __PLACE_YEAR__" >&1

CHUNK_DIR="$DATA_DIR/interim/chunks/__PLACE_YEAR__"

echo "CHUNK: $CHUNK_DIR"

CHUNK_FILE=$(printf "%s/chunk_%02d" "$CHUNK_DIR" $((SGE_TASK_ID-1)))

# ファイル名の配列に読み込んで Python に渡す
mapfile -t FILES < "$CHUNK_FILE"
python3 /home/fukui/workspace/TravelModeEstimation/scripts/03_frequency/gps_sample.py "${FILES[@]}"
