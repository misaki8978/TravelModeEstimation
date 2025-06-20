#!/usr/bin/env bash
# 01_make_chunks.sh
#$ -S /bin/bash


echo "Using place_year: $PLACE_YEAR"

# Split __PLACE_YEAR__ into place and year
place_year="$PLACE_YEAR"
PLACE=$(echo "$place_year" | sed 's/_[^_]*$//')  # Gets everything before the last underscore
YEAR=$(echo "$place_year" | sed 's/.*_//')       # Gets everything after the last underscore

echo "Using place: $PLACE" >&1
echo "Using year: $YEAR" >&1
echo "Using place_year: $PLACE_YEAR" >&1

# データがあるディレクトリ（環境に合わせて調整）
# DIR="$COMMON_DIR/BLWSakigake/${PLACE_YEAR}"  #本番
DIR="$DATA_DIR/processed/04_01_${PLACE}/${YEAR}_weekly"
# DIR="$DATA_DIR/interim/filtered/${PLACE}/${YEAR}_weekly/user_counts_4500/sorted"
# DIR="$DATA_DIR/${PLACE_YEAR}/"  #test
# DIR="$DATA_DIR/${PLACE_YEAR}/"  #test
# チャンクファイルを置くフォルダ
CHUNK_DIR="$DATA_DIR/interim/chunks/${PLACE_YEAR}_segment"
CHUNK=1
mkdir -p "$CHUNK_DIR"
rm -rf "$CHUNK_DIR/chunk_*"    # 古いチャンクがあれば削除


# データファイルの一覧を作成
find "$DIR" -maxdepth 1 -name '*GPS.csv.gz' | sort > "$CHUNK_DIR/all_files.lst"

# 一覧を100行ごとに分割
split -d -l "$CHUNK" "$CHUNK_DIR/all_files.lst" "$CHUNK_DIR/chunk_"
echo "==> チャンク数: $(ls "$CHUNK_DIR"/chunk_* | wc -l)"
