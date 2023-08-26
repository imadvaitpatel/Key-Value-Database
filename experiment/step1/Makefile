CC = g++ -g -std=c++17 -D_GLIBCXX_DEBUG
CFLAGS = -c -g3 -Wall -Wextra -Werror -O3 -pedantic -fsanitize=address,undefined,leak -fno-omit-frame-pointer

all: step1_experiments

step1_experiments: step1_experiments.cpp ../../src/db.cpp ../../src/avl_tree.cpp ../../src/sst.cpp
	$(CC) $^ -o $@

clean:
	rm -rf *.o step1_experiments step1_db*

