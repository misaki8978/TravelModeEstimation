#!/usr/bin/env bash
# 01_make_chunks.sh
#$ -S /bin/bash


# echo "Using place_year: $PLACE_YEAR"

# データがあるディレクトリ（環境に合わせて調整）
DIR="$COMMON_DIR/BLWSakigake/${PLACE_YEAR}"  #本番
# DIR="$DATA_DIR/${PLACE_YEAR}/"  #test
# チャンクファイルを置くフォルダ
CHUNK_DIR="$DATA_DIR/interim/chunks/${PLACE_YEAR}"
CHUNK=100
mkdir -p "$CHUNK_DIR"
rm -rf "$CHUNK_DIR/chunk_*"    # 古いチャンクがあれば削除


# データファイルの一覧を作成
find "$DIR" -maxdepth 1 -name '*.csv.gz' | sort > "$CHUNK_DIR/all_files.lst"

# 一覧を100行ごとに分割
split -d -l "$CHUNK" "$CHUNK_DIR/all_files.lst" "$CHUNK_DIR/chunk_"
echo "==> チャンク数: $(ls "$CHUNK_DIR"/chunk_* | wc -l)"
