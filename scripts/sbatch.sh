#!/bin/bash

#SBATCH --job-name=${FILL ME}
#SBATCH --mail-type=ALL
#SBATCH --mail-user=fzli@ucdavis.edu
#SBATCH --output=/home/lfz/git/${FILL ME}/scripts/logs/%j.out
#SBATCH --error=/home/lfz/git/${FILL ME}/scripts/logs/%j.err
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --gres gpu:1
#SBATCH --mem=8G
#SBATCH --time=10:00:00

cd ..
${FILL ME}
