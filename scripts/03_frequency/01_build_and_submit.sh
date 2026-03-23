#$ -cwd
#$ -S /bin/bash
#$ -V
#$ -N gps_weekly
#$ -q all.q@Cheryl
#$ -pe smp 8
#$ -tc 3

# 01_build_and_submit.sh

cd $HOME/workspace/TravelModeEstimation

place_year="09_nagasaki_2022b"  #ここを変更！
echo "place_year=${place_year}" >&2

PLACE_YEAR=${place_year} bash ./scripts/03_frequency/01_make_chunks.sh >&2


mkdir -p logs/03_frequency/${place_year}
rm -rf logs/03_frequency/${place_year}/*
mkdir -p logs/${place_year}

CHUNK_DIR="$DATA_DIR/interim/chunks/${place_year}"

echo ${CHUNK_DIR}
CHUNKS=$(ls ${CHUNK_DIR}/chunk_* | wc -l)
sed -e "s/__NUM__/$CHUNKS/" -e "s/__PLACE_YEAR__/$place_year/" ./scripts/03_frequency/01_array_template.sh > ./scripts/03_frequency/01_gps_array.sh

qsub ./scripts/03_frequency/01_gps_array.sh
