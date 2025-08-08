#$ -cwd
#$ -S /bin/bash
#$ -V
#$ -N gis_layer
#$ -q all.q@Jake

# 01_build_and_submit.sh

rm logs/log_09_gis.txt

cd $HOME/workspace/TravelModeEstimation

place_year="09_nagasaki_2019"  #ここを変更！
echo "place_year=${place_year}" >&2

PLACE_YEAR=${place_year} bash ./scripts/09_gis/01_make_chunks.sh >&2


mkdir -p logs/${place_year}
rm -rf logs/${place_year}/*

CHUNK_DIR="$DATA_DIR/interim/chunks/${place_year}_gis"

echo ${CHUNK_DIR}
CHUNKS=$(ls ${CHUNK_DIR}/chunk_* | wc -l)
echo "CHUNKS=${CHUNKS}" >&2
sed -e "s/__NUM__/$CHUNKS/" -e "s/__PLACE_YEAR__/$place_year/" ./scripts/09_gis/01_array_template.sh > ./scripts/09_gis/01_gis_array.sh

qsub ./scripts/09_gis/01_gis_array.sh
