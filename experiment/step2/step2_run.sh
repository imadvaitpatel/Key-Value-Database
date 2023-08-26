#!/bin/bash
cd ../.. && make clean && make && cd -
make clean && make
./step2_experiments
python3 step2_graphs.py
make clean