# 02_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N simple_work
#$ -q all.q@Dwight
#$ -pe smp 10
#$ -o logs/simple_work.out
#$ -e logs/simple_work.err

place_year="07_osaka_2019"  #ここを変更！

DATA_DIR1="/home/data/fukui/interim/agg_before_filter/${place_year}/bulk/"
DATA_DIR2="/home/data/fukui/interim/agg_before_filter/${place_year}b/bulk/"

week_frequencys=($(find "${DATA_DIR1}" "${DATA_DIR2}" -type f -name "*.csv.gz"))

python3 /home/fukui/workspace/TravelModeEstimation/scripts/03_frequency/02_week_frequency_merge.py "${week_frequencys[@]}"