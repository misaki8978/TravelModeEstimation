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
# DIR="$DATA_DIR/processed/04_01_${PLACE}/${YEAR}_weekly"  #nagasaki ver.
# DIR="$DATA_DIR/interim/multithread/04_01_${PLACE}_${YEAR}_weekly/hariharan/"  #hariharan ver.
DIR="$DATA_DIR/interim/multithread/04_01_${PLACE_YEAR}_weekly/basic/"  #osaka ver.

# チャンクファイルを置くフォルダ
CHUNK_DIR="$DATA_DIR/interim/chunks/${PLACE_YEAR}_segment"
CHUNK=1

rm -rf "$CHUNK_DIR"    # 古いチャンクがあれば削除
mkdir -p "$CHUNK_DIR"




# データファイルの一覧を作成
# find "$DIR" -maxdepth 1 -name '*GPS.csv.gz' | sort > "$CHUNK_DIR/all_files.lst"
find "$DIR" -maxdepth 1 -name 'speed*.csv' | sort > "$CHUNK_DIR/all_files.lst"

# 一覧を100行ごとに分割
split -d -l "$CHUNK" "$CHUNK_DIR/all_files.lst" "$CHUNK_DIR/chunk_"
echo "==> チャンク数: $(ls "$CHUNK_DIR"/chunk_* | wc -l)"
