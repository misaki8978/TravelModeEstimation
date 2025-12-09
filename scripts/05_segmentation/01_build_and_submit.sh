#$ -cwd
#$ -S /bin/bash
#$ -V
#$ -N gps_segmentation
#$ -q all.q@Claudette

# 01_build_and_submit.sh



cd $HOME/workspace/TravelModeEstimation

place_year="09_nagasaki_2019"  #ここを変更！
echo "place_year=${place_year}" >&2



# rm logs/05_segment/${place_year}.txt
mkdir -p "./logs/05_segment/${place_year}"
rm -rf "./logs/05_segment/${place_year}/*"

PLACE_YEAR=${place_year} bash ./scripts/05_segmentation/01_make_chunks.sh >&2

CHUNK_DIR="$DATA_DIR/interim/chunks/${place_year}_segment"

echo ${CHUNK_DIR}
CHUNKS=$(ls ${CHUNK_DIR}/chunk_* | wc -l)
echo "CHUNKS=${CHUNKS}" >&2
sed -e "s/__NUM__/$CHUNKS/" -e "s/__PLACE_YEAR__/$place_year/" ./scripts/05_segmentation/01_array_template.sh > ./scripts/05_segmentation/01_segment_array.sh

qsub ./scripts/05_segmentation/01_segment_array.sh
