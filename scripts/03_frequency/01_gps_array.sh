# 01_array_template.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N gps_weekly
#$ -q all.q@Dwight
#$ -pe smp 8
#$ -tc 3
#$ -t 1-50
#$ -o logs/03_frequency/04_kanagawa_2021/$TASK_ID.out
#$ -e logs/03_frequency/04_kanagawa_2021/$TASK_ID.err


echo "Using place_year: 04_kanagawa_2021" >&1

CHUNK_DIR="$DATA_DIR/interim/chunks/04_kanagawa_2021"

echo "CHUNK: $CHUNK_DIR"

CHUNK_FILE=$(printf "%s/chunk_%02d" "$CHUNK_DIR" $((SGE_TASK_ID-1)))

# ファイル名の配列に読み込んで Python に渡す
mapfile -t FILES < "$CHUNK_FILE"
python3 /home/fukui/workspace/TravelModeEstimation/scripts/03_frequency/01_tokyo_gps_sample.py "${FILES[@]}"
