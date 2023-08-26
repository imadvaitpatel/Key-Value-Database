CC = g++ -g -std=c++17 -D_GLIBCXX_DEBUG
CFLAGS = -c -Wall -Wextra -Werror -O3 -pedantic -fsanitize=address,undefined,leak -fno-omit-frame-pointer

all: db_test avl_tree_test kvpair_test sst_test buffer_pool_test

db_test: tests/db_test.cpp src/db.cpp src/avl_tree.cpp src/sst.cpp src/kvpair.cpp src/buffer_pool.cpp src/clock_replacer.cpp src/lru_replacer.cpp src/db.h src/avl_tree.h
	$(CC) $^ -o $@

avl_tree_test: tests/avl_tree_test.cpp src/avl_tree.cpp src/avl_tree.h
	$(CC) $^ -o $@

kvpair_test: tests/kvpair_test.cpp src/kvpair.cpp src/kvpair.h
	$(CC) $^ -o $@

sst_test: tests/sst_test.cpp src/sst.cpp src/sst.h src/clock_replacer.cpp src/clock_replacer.h src/lru_replacer.cpp src/lru_replacer.h src/buffer_pool.cpp src/buffer_pool.h
	$(CC) $^ -o $@

buffer_pool_test: tests/buffer_pool_test.cpp src/clock_replacer.cpp src/clock_replacer.h src/lru_replacer.cpp src/lru_replacer.h src/buffer_pool.cpp src/buffer_pool.h
	$(CC) $^ -o $@

%.o: %.cpp
	$(CC) $(CFLAGS) -o $@ $<

test:
	@ ./avl_tree_test && \
		./kvpair_test && \
		./sst_test && \
		./db_test && \
		./buffer_pool_test && \
		echo "ALL TESTS PASSED!! 😊"

clean:
	rm -rf *.o avl_tree_test kvpair_test sst_test db_test buffer_pool_test *.sst
