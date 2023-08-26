#!/bin/bash
cd ../.. && make clean && make && cd -
make clean && make
./step1_experiments
python3 step1_graphs.py
make clean