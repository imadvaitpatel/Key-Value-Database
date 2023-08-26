#include "../src/db.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>

#include "../src/exceptions.h"

using namespace std;
namespace fs = std::filesystem;

void cleanup() {
  // Removing test data from previous runs
  fs::remove_all("TEST_DB");
  fs::remove_all("TEST_PG");
  fs::remove_all("TEST_UD");
  fs::remove_all("TEST_SCAN");
  fs::remove_all("SORTED_SSTS");
  fs::remove_all("TEST_BIG_ONE_SST");
  fs::remove_all("TEST_BIG_MANY_SSTS");
}

void test_open_close() {
  DB db;
  int memtable_size = 7834;
  assert(db.open("TEST_DB", memtable_size));

  assert(fs::is_directory("TEST_DB"));
  assert(db.metadata.next_sst_id == 0);
  assert(db.metadata.num_elems == 0);
  assert(db.sst_names.size() == 0);

  // Closing and reopening DB without any transactions should not change
  // metadata
  assert(db.close());
  assert(db.open("TEST_DB"));
  assert(db.metadata.next_sst_id == 0);
  assert(db.metadata.num_elems == 0);
  assert(db.sst_names.size() == 0);

  db.put(1, 2);
  db.put(3, 4);

  int sst_id_after_close = 1;
  string sst_name = "TEST_DB/0" + SST_EXTENSION;
  int num_elems = 2;

  assert(db.close());

  assert(fs::exists("TEST_DB/" + METADATA_FILE));
  assert(fs::exists(sst_name));

  DB db2;
  assert(db2.open("TEST_DB"));

  assert(fs::is_directory("TEST_DB"));

  // Check for persistence of metadata when database is reopened
  assert(db2.metadata.memtable_size == memtable_size);
  assert(db2.metadata.next_sst_id == sst_id_after_close);
  assert(db2.metadata.num_elems == num_elems);
  assert(db2.sst_names.size() == 1 && db2.sst_names[0] == sst_name);
  assert(db2.close());
}

void test_put_get() {
  DB db;
  db.open("TEST_PG", 2);

  db.put(1, 2);
  db.put(2, 3);

  assert(fs::exists("TEST_PG/0.sst"));
  assert(db.get(1) == 2);
  assert(db.get(2) == 3);

  db.put(3, 4);
  assert(db.get(3) == 4);
  db.put(4, 5);

  assert(fs::exists("TEST_PG/1.sst"));
  assert(db.get(1) == 2);
  assert(db.get(2) == 3);
  assert(db.get(4) == 5);

  try {
    db.get(9);
    assert(0);  // shouldn't get here
  } catch (KeyException& e) {
  }

  db.close();
}

void test_update() {
  DB db;
  db.open("TEST_UD", 2);

  db.put(1, 2);
  db.put(1, 3);
  assert(db.get(1) == 3);

  db.close();
}

void test_scan() {
  DB db;
  db.open("TEST_SCAN", 3);

  vector<KVPair> kv_pairs = {{.key = 2, .value = 3},  {.key = 4, .value = 5},
                             {.key = 5, .value = 6},  {.key = 7, .value = 8},
                             {.key = 9, .value = 10}, {.key = 11, .value = 12},
                             {.key = 13, .value = 14}};

  for (auto pair : kv_pairs) {
    db.put(pair.key, pair.value);
  }

  vector<KVPair> scanned = db.scan(MIN_KEY, MAX_KEY);

  // Verify all results were returned in the scan
  assert(scanned.size() == kv_pairs.size());
  for (auto s : scanned) {
    auto it = find_if(kv_pairs.begin(), kv_pairs.end(), [&s](const auto& p) {
      return p.key == s.key && p.value == s.value;
    });
    assert(it != kv_pairs.end());
  }

  vector<KVPair> scanned_range = db.scan(5, 10);
  assert(scanned_range.size() == 3);
  for (auto s : scanned_range) {
    auto it = find_if(kv_pairs.begin(), kv_pairs.end(), [&s](const auto& p) {
      return p.key == s.key && p.value == s.value;
    });
    assert(it != kv_pairs.end());
  }

  db.close();
}

void test_sorted_ssts() {
  DB db;
  int memtable_size = 5;
  int num_ssts = 10;
  int num_elems = memtable_size * num_ssts;
  assert(db.open("SORTED_SSTS", memtable_size));

  for (int i = 0; i < num_elems; i++) {
    db.put(i, i);
  }

  // Verify SSTs are sorted
  assert(db.sst_names.size() == num_ssts);
  for (int i = 0; i < num_ssts; i++) {
    string expected_name = "SORTED_SSTS/" + to_string(i) + SST_EXTENSION;
    assert(db.sst_names[i] == expected_name);
  }

  // SSTs should still be sorted when DB is reopened
  assert(db.close());
  assert(db.open("SORTED_SSTS"));
  assert(db.sst_names.size() == num_ssts);
  for (int i = 0; i < num_ssts; i++) {
    string expected_name = "SORTED_SSTS/" + to_string(i) + SST_EXTENSION;
    assert(db.sst_names[i] == expected_name);
  }

  db.close();
}

void test_big_data_one_sst() {
  DB db;
  uint64_t size = 10000;
  db.open("TEST_BIG_ONE_SST", size);

  vector<KVPair> pairs;
  for (uint64_t i = 0; i < size; i++) {
    KVPair pair = {.key = i, .value = i + size};
    pairs.push_back(pair);
  }

  random_shuffle(pairs.begin(), pairs.end());

  for (int i = 0; i < size; i++) {
    db.put(pairs[i].key, pairs[i].value);
  }

  assert(db.sst_names.size() == 1);

  assert(db.get(100) == 100 + size);
  assert(db.get(4430) == 4430 + size);
  assert(db.get(size - 1) == size - 1 + size);

  vector<KVPair> res = db.scan(MIN_KEY, MAX_KEY);

  // Verify that result is sorted
  uint64_t nextKey = 0;
  for (int i = 0; i < size; i++) {
    assert(res[i].key == nextKey);
    assert(res[i].value == nextKey + size);
    nextKey++;
  }

  db.close();
}

void test_big_data_many_ssts() {
  DB db;
  uint64_t size = 10000;
  int num_ssts = 100;
  int memtable_size = size / num_ssts;
  db.open("TEST_BIG_MANY_SSTS", memtable_size);

  vector<KVPair> pairs;
  for (uint64_t i = 0; i < size; i++) {
    KVPair pair = {.key = i, .value = i + size};
    pairs.push_back(pair);
  }

  random_shuffle(pairs.begin(), pairs.end());

  for (int i = 0; i < size; i++) {
    db.put(pairs[i].key, pairs[i].value);
  }

  assert(db.sst_names.size() == num_ssts);

  vector<KVPair> all_pairs = db.scan(MIN_KEY, MAX_KEY);
  assert(all_pairs.size() == size);

  // Verify that every pair is in the output
  // result does NOT have to be sorted (maybe later?)
  for (int i = 0; i < size; i++) {
    assert(all_pairs[i].value == all_pairs[i].key + size);
  }

  vector<KVPair> scanned_range = db.scan(333, 999);
  assert(scanned_range.size() == (999 - 333) + 1);
  for (int i = 333; i <= 999; i++) {
    auto it = find_if(scanned_range.begin(), scanned_range.end(),
                      [&size, &i](const auto& s) {
                        return s.key == i && s.value == i + size;
                      });
    assert(it != scanned_range.end());
  }

  db.close();
}

int main() {
  cleanup();

  test_open_close();
  test_put_get();
  test_update();
  test_scan();
  test_sorted_ssts();
  test_big_data_one_sst();
  test_big_data_many_ssts();

  cleanup();
  cout << "DB tests passed!\n";
  return 0;
}
