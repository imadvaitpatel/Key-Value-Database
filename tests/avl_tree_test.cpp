#include "../src/avl_tree.h"

#include <cassert>
#include <iostream>

#include "../src/exceptions.h"

using namespace std;

void test_get_put() {
  Tree memtable(5);
  assert(memtable.put(4, 4));
  assert(memtable.put(2, 5));
  assert(memtable.put(1, 7));
  assert(memtable.put(8, 3));

  assert(memtable.get(2) == 5);
  assert(memtable.get(1) == 7);
  assert(memtable.get(4) == 4);
  assert(memtable.get(8) == 3);

  try {
    memtable.get(9);
    assert(0);  // shouldn't get here
  } catch (KeyException &e) {
  }

  assert(!memtable.put(3, 6));
}

void test_scan() {
  Tree memtable(5);
  vector<KVPair> sorted_kv_pairs = {
      {.key = 2, .value = 3},
      {.key = 4, .value = 5},
      {.key = 5, .value = 6},
      {.key = 7, .value = 8},
  };

  for (auto pair : sorted_kv_pairs) {
    memtable.put(pair.key, pair.value);
  }

  vector<KVPair> all_scanned = memtable.scan(MIN_KEY, MAX_KEY);

  // Check if scanned results are sorted
  assert(all_scanned.size() == sorted_kv_pairs.size());
  for (int i = 0; i < all_scanned.size(); i++) {
    auto expected = sorted_kv_pairs[i];
    auto actual = all_scanned[i];
    assert(expected.key == actual.key);
    assert(expected.value == actual.value);
  }

  sorted_kv_pairs = {
      {.key = 4, .value = 5},
      {.key = 5, .value = 6},
  };
  vector<KVPair> range_scanned = memtable.scan(3, 6);

  // Check if scanned results are sorted
  assert(range_scanned.size() == sorted_kv_pairs.size());
  for (int i = 0; i < range_scanned.size(); i++) {
    auto expected = sorted_kv_pairs[i];
    auto actual = range_scanned[i];
    assert(expected.key == actual.key);
    assert(expected.value == actual.value);
  }
}

int main() {
  test_get_put();
  test_scan();
  cout << "AVL tree tests passed!\n";
  return 0;
}
