#!/usr/bin/env bash
# make_chunks.sh
#$ -S /bin/bash


place_year="data_10"  #ここを変更！
echo "[INFO] place_year=${place_year}" >&2
# データがあるディレクトリ（環境に合わせて調整）

# DIR="$COMMON_DIR/BLWSakigake/${place_year}"  #本番
DIR="$DATA_DIR/interim/agg_before_filter/${place_year}/bulk"  #test
# チャンクファイルを置くフォルダ
CHUNK_DIR="$DATA_DIR/interim/chunks/${place_year}"
CHUNK=2

mkdir -p "$CHUNK_DIR"
rm -rf "$CHUNK_DIR/chunk_*"    # 古いチャンクがあれば削除

# データファイルの一覧を作成
find "$DIR" -maxdepth 1 -name '*.csv.gz' | sort > "$CHUNK_DIR/all_files.lst"

# 一覧を100行ごとに分割
split -d -l "$CHUNK" "$CHUNK_DIR/all_files.lst" "$CHUNK_DIR/chunk_"

echo "==> チャンク数: $(ls "$CHUNK_DIR"/chunk_* | wc -l)"
