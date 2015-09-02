#!/bin/bash

RESULT_DIR="write_1_run1"

rm -r $RESULT_DIR
mkdir $RESULT_DIR

machines=( 1 2 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60 64 66 )
threads=( 1 2 4 8 12 16 )

for n in "${machines[@]}"; do
        for t in "${threads[@]}"; do
                sbatch --exclusive --time=15 --nodes=$n --error=$RESULT_DIR/result-$n-$t.err --output=$RESULT_DIR/result-$n-$t.out submit.sh $n $t $((n*t))

                while [ $(squeue -h -uoortwijn | wc -l) -gt 0 ]; do
                        sleep 10
                done
        done
done



