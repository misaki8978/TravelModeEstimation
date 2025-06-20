# array_template.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N gps_weekly
#$ -q all.q@Claudette
#$ -pe smp 1
#$ -t 1-3
#$ -o logs/$TASK_ID.out
#$ -e logs/$TASK_ID.err

place_year="data_10"  #ここを変更！

CHUNK_DIR="$DATA_DIR/interim/chunks/${place_year}"

CHUNK_FILE=$(printf "%s/chunk_%02d" "$CHUNK_DIR" $((SGE_TASK_ID-1)))

# ファイル名の配列に読み込んで Python に渡す
mapfile -t FILES < "$CHUNK_FILE"
python3 gps_sample.py "${FILES[@]}"
