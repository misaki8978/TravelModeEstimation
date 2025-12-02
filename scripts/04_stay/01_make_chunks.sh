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
DIR="$DATA_DIR/interim/filtered/${PLACE}/${YEAR}_weekly/user_counts_4500/sorted"
# DIR="$DATA_DIR/${PLACE_YEAR}/"  #test
# DIR="$DATA_DIR/${PLACE_YEAR}/"  #test
# チャンクファイルを置くフォルダ
CHUNK_DIR="$DATA_DIR/interim/chunks/${PLACE_YEAR}_gps"

 # 古いチャンクがあれば削除
rm -rf "$CHUNK_DIR/"  
mkdir -p "$CHUNK_DIR"



# 月ごとにファイルをまとめる
# ファイル名: {month}_sorted_{i}.csv.gz を想定
find "$DIR" -maxdepth 1 -name '*.csv.g*' -print0 | while IFS= read -r -d '' file; do
    base=$(basename "$file")
    # {month}_sorted_... の {month} 部分を取り出す
    month="${base%%_sorted_*}"     # 例: 2019-01_sorted_1.csv.gz -> 2019-01

    chunk_file="$CHUNK_DIR/chunk_${month}.lst"
    echo "$file" >> "$chunk_file"
done

echo "==> チャンク数: $(ls "$CHUNK_DIR"/chunk_*.lst 2>/dev/null | wc -l)"
echo "生成されたチャンク一覧:"
ls "$CHUNK_DIR"/chunk_*.lst 2>/dev/null || echo "チャンクファイルがありません"