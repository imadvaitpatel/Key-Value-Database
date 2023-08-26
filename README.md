# Summary
This is a key-value store implementing common operations in NoSQL databases. It includes database concepts such as an in-memory memtable, efficient storage using Sorted String Tables (SSTs), buffer pool with Clock and LRU eviction policies, and B-trees. The performance of these optimizations are tested in experiments.

# How To Run
To run tests, simply run ```make test``` in the root directory.

To run experiments:
```
make
cd experiment/step1 # or cd experiment/step2
make 
sh run_step1.sh # or sh run_step2.sh
```
