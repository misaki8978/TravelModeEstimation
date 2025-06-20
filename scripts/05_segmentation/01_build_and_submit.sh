#$ -cwd
#$ -S /bin/bash
#$ -V
#$ -N gps_segmentation
#$ -q all.q@Cheryl

# 01_build_and_submit.sh

rm logs/log_05_segmentation.txt

cd $HOME/workspace/TravelModeEstimation

place_year="09_nagasaki_2019"  #ここを変更！
echo "place_year=${place_year}" >&2

PLACE_YEAR=${place_year} bash ./scripts/05_segmentation/01_make_chunks.sh >&2


mkdir -p logs/${place_year}_segment
rm -rf logs/${place_year}_segment/*

CHUNK_DIR="$DATA_DIR/interim/chunks/${place_year}_segment"

echo ${CHUNK_DIR}
CHUNKS=$(ls ${CHUNK_DIR}/chunk_* | wc -l)
echo "CHUNKS=${CHUNKS}" >&2
sed -e "s/__NUM__/$CHUNKS/" -e "s/__PLACE_YEAR__/$place_year/" ./scripts/05_segmentation/01_array_template.sh > ./scripts/05_segmentation/01_segment_array.sh

qsub ./scripts/05_segmentation/01_segment_array.sh
