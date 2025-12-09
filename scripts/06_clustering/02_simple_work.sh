# 02_simple_work.sh
#!/usr/bin/env bash
#$ -S /bin/bash
#$ -cwd
#$ -V
#$ -N clustering
#$ -q all.q@Cheryl
#$ -pe smp 8
#$ -o logs/06_clustering/clustering.out
#$ -e logs/06_clustering/clustering.err

PLACE="03_tokyo"
YEAR="2019"

# INPUT_DIR="$DATA_DIR/processed/09_04_re/${PLACE}/${YEAR}_weekly"
INPUT_DIR="$DATA_DIR/processed/09_04_re/${PLACE}/${YEAR}_weekly"
INPUT_FILES=(${INPUT_DIR}/*nonwalk.csv.gz)

python3 /home/fukui/workspace/TravelModeEstimation/scripts/06_clustering/02_clustering.py "${INPUT_FILES[@]}"



