#$ -cwd
#$ -S /bin/bash
#$ -V
#$ -N stay_detection
#$ -q all.q@Claudette
#$ -tc 3
# 01_build_and_submit.sh
place_year="09_nagasaki_2019"  #ここを変更！
echo "place_year=${place_year}" >&2
# rm logs/${place_year}/log_04_stay_multithread.txt

cd $HOME/workspace/TravelModeEstimation



PLACE_YEAR=${place_year} bash ./scripts/04_stay/01_make_chunks.sh >&2

mkdir -p logs/04_stay/${place_year}
rm -rf logs/04_stay/${place_year}/*


CHUNK_DIR="$DATA_DIR/interim/chunks/${place_year}_gps"

echo ${CHUNK_DIR}
CHUNKS=$(ls ${CHUNK_DIR}/chunk_* | wc -l)
echo "CHUNKS=${CHUNKS}" >&2
sed -e "s/__NUM__/$CHUNKS/" -e "s/__PLACE_YEAR__/$place_year/" ./scripts/04_stay/01_array_template.sh > ./scripts/04_stay/01_stay_array.sh

qsub ./scripts/04_stay/01_stay_array.sh