# 01_array_template.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N gps_weekly
#$ -q all.q@Dwight
#$ -pe smp 8
#$ -tc 5
#$ -t 1-66
#$ -o logs/07_osaka_2019b/$TASK_ID.out
#$ -e logs/07_osaka_2019b/$TASK_ID.err


echo "Using place_year: 07_osaka_2019b" >&1

CHUNK_DIR="$DATA_DIR/interim/chunks/07_osaka_2019b"

echo "CHUNK: $CHUNK_DIR"

CHUNK_FILE=$(printf "%s/chunk_%02d" "$CHUNK_DIR" $((SGE_TASK_ID-1)))

# ファイル名の配列に読み込んで Python に渡す
mapfile -t FILES < "$CHUNK_FILE"
python3 /home/fukui/workspace/TravelModeEstimation/scripts/03_frequency/01_tokyo_gps_sample.py "${FILES[@]}"
