CC = g++ -g -std=c++17 -D_GLIBCXX_DEBUG
CFLAGS = -c -g3 -Wall -Wextra -Werror -O3 -pedantic -fsanitize=address,undefined,leak -fno-omit-frame-pointer

all: step2_experiments

step2_experiments: step2_experiments.cpp ../../src/db.cpp ../../src/tree.cpp ../../src/sst.cpp ../../src/buffer_pool.cpp ../../src/clock_replacer.cpp ../../src/lru_replacer.cpp
	$(CC) $^ -o $@

clean:
	rm -rf *.o step2_experiments step2_db*