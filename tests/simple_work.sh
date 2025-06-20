# simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N simple_work
#$ -q all.q@Claudette
#$ -pe smp 1
#$ -o logs/simple_work.out
#$ -e logs/simple_work.err

place_year="data_10"  #ここを変更！

DATA_DIR="/home/data/fukui/interim/agg_before_filter/${place_year}/bulk/"

week_frequencys=(${DATA_DIR}*.csv.gz)

python3 /home/fukui/workspace/TravelModeEstimation/tests/week_frequency_merge.py "${week_frequencys[@]}"