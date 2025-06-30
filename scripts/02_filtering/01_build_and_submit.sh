#$ -cwd
#$ -S /bin/bash
#$ -V
#$ -N gps_weekly
#$ -q all.q@Cheryl

# 01_build_and_submit.sh

cd $HOME/workspace/TravelModeEstimation

place_year="09_nagasaki_2019"  #ここを変更！
echo "place_year=${place_year}" >&1

PLACE_YEAR=${place_year} bash ./scripts/02_filtering/01_make_chunks.sh >&1


mkdir -p logs/${place_year}
rm -rf logs/${place_year}/*

CHUNK_DIR="$DATA_DIR/interim/chunks/${place_year}"

echo ${CHUNK_DIR} >&1
CHUNKS=$(ls ${CHUNK_DIR}/chunk_* | wc -l)
sed -e "s/__NUM__/$CHUNKS/" -e "s/__PLACE_YEAR__/$place_year/" ./scripts/02_filtering/01_array_template.sh > ./scripts/02_filtering/01_gps_array.sh

qsub ./scripts/02_filtering/01_gps_array.sh


