#$ -cwd
#$ -S /bin/bash
#$ -V
#$ -N gps_weekly
#$ -q all.q@Claudette

# build_and_submit.sh

cd $HOME/workspace/TravelModeEstimation

bash ./tests/make_chunks.sh


place_year="data_10"  #ここを変更！
mkdir -p logs/${place_year}

CHUNK_DIR="$DATA_DIR/interim/chunks/${place_year}"

echo "data_10=${place_year}" >&2

CHUNKS=$(ls ${CHUNK_DIR}/chunk_* | wc -l)
sed -e "s/__NUM__/$CHUNKS/" -e "s/__PLACE_YEAR__/$place_year/" ./tests/array_template.sh > ./tests/gps_array.sh

qsub ./tests/gps_array.sh
