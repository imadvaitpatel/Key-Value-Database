#include "../src/kvpair.h"

#include <cassert>
#include <iostream>
#include <vector>

using namespace std;

void test_empty_both() {
  vector<KVPair> kvps1, kvps2;
  auto actual = merge(kvps1, kvps2);
  assert(actual.empty());
}

void test_empty_1() {
  vector<KVPair> kvps1 = {{1, 10}, {2, 20}, {3, 30}};
  vector<KVPair> kvps2 = {};
  auto actual = merge(kvps1, kvps2);
  assert(actual == kvps1);
}

void test_empty_2() {
  vector<KVPair> kvps1 = {};
  vector<KVPair> kvps2 = {{1, 10}, {2, 20}, {3, 30}};
  auto actual = merge(kvps1, kvps2);
  assert(actual == kvps2);
}

void test_unique() {
  vector<KVPair> kvps1 = {{1, 10}, {4, 40}, {6, 60}};
  vector<KVPair> kvps2 = {{2, 20}, {3, 30}, {5, 50}};
  vector<KVPair> expected = {{1, 10}, {2, 20}, {3, 30},
                             {4, 40}, {5, 50}, {6, 60}};
  auto actual = merge(kvps1, kvps2);
  assert(actual == expected);
}

void test_duplicate() {
  vector<KVPair> kvps1 = {{1, 10}, {4, 40}, {6, 60}};
  vector<KVPair> kvps2 = {{2, 20}, {4, 30}, {5, 50}};
  vector<KVPair> expected = {{1, 10}, {2, 20}, {4, 40}, {5, 50}, {6, 60}};
  auto actual = merge(kvps1, kvps2);
  assert(actual == expected);
}

void test_duplicate_and_longer1() {
  vector<KVPair> kvps1 = {{1, 10}, {4, 40}, {6, 60}, {7, 70}, {8, 80}};
  vector<KVPair> kvps2 = {{2, 20}, {4, 30}, {5, 50}};
  vector<KVPair> expected = {{1, 10}, {2, 20}, {4, 40}, {5, 50},
                             {6, 60}, {7, 70}, {8, 80}};
  auto actual = merge(kvps1, kvps2);
  assert(actual == expected);
}

void test_duplicate_and_longer2() {
  vector<KVPair> kvps1 = {{1, 10}, {4, 40}, {6, 60}};
  vector<KVPair> kvps2 = {{2, 20}, {4, 30}, {5, 50}, {7, 70}, {8, 80}};
  vector<KVPair> expected = {{1, 10}, {2, 20}, {4, 40}, {5, 50},
                             {6, 60}, {7, 70}, {8, 80}};
  auto actual = merge(kvps1, kvps2);
  assert(actual == expected);
}

int main() {
  test_empty_both();
  test_empty_1();
  test_empty_2();
  test_unique();
  test_duplicate();
  test_duplicate_and_longer1();
  test_duplicate_and_longer2();
  cout << "KVPair tests passed!\n";
  return 0;
}
