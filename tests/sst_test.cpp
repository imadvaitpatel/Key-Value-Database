#include "../src/sst.h"

#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>

#include <cassert>
#include <filesystem>
#include <iostream>
#include <vector>

using namespace std;
namespace fs = std::filesystem;

void compare_kv_pair(KVPair expected, KVPair actual) {
  assert(expected.key == actual.key);
  assert(expected.value == actual.value);
}

void test_sst_read_write_newfile() {
  string filename = "test_sst_read_write_newfile.sst";
  vector<KVPair> kv_pairs = {
      {.key = 2, .value = 3},
      {.key = 4, .value = 5},
      {.key = 5, .value = 6},
  };

  write_sst(kv_pairs, filename);
  vector<KVPair> result = read_sst(filename);

  for (int i = 0; i < kv_pairs.size(); i++) {
    compare_kv_pair(kv_pairs[i], result[i]);
  }
  fs::remove(filename);
}

void test_sst_read_write_existing() {
  string filename = "test_sst_read_write_existing.sst";
  vector<KVPair> kv_one = {{.key = 7, .value = 8}};
  vector<KVPair> kv_two = {{.key = 8, .value = 9}};

  write_sst(kv_one, filename);
  write_sst(kv_two, filename);

  vector<KVPair> result = read_sst(filename);
  compare_kv_pair(kv_two[0], result[0]);

  vector<KVPair> kv_three = {{.key = 10, .value = 11}};

  write_sst(kv_three, filename);
  result = read_sst(filename);
  compare_kv_pair(kv_three[0], result[0]);

  fs::remove(filename);
}

void test_sst_big() {
  string filename = "test_sst_big.sst";
  uint64_t size = 10000;

  vector<KVPair> pairs;
  for (uint64_t i = 0; i < size; i++) {
    KVPair pair = {.key = i, .value = i + size};
    pairs.push_back(pair);
  }

  write_sst(pairs, filename);

  vector<KVPair> res = read_sst(filename);

  int nextKey = 0;

  for (int i = 0; i < size; i++) {
    compare_kv_pair(pairs[0], res[0]);
    nextKey++;
  }

  fs::remove(filename);
}

int main() {
  test_sst_read_write_newfile();
  test_sst_read_write_existing();
  test_sst_big();
  cout << "SST tests passed!\n";
  return 0;
}
