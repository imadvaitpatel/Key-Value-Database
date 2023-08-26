#include "kvpair.h"

#include <algorithm>
#include <vector>

using namespace std;

vector<KVPair> merge(vector<KVPair> kvps1, vector<KVPair> kvps2) {
  vector<KVPair> merged;

  auto it1 = kvps1.begin();
  auto it2 = kvps2.begin();

  while (it1 != kvps1.end() && it2 != kvps2.end()) {
    if (it1->key < it2->key && it1->value != TOMBSTONE) {
      merged.push_back(*it1);
      ++it1;
    } else if (it2->key < it1->key && it2->value != TOMBSTONE) {
      merged.push_back(*it2);
      ++it2;
    } else if (it1->value != TOMBSTONE) {
      merged.push_back(*it1);
      ++it1;
      ++it2;
    }
  }

  while (it1 != kvps1.end()) {
    if (it1->value != TOMBSTONE) {
      merged.push_back(*it1);
    }
    ++it1;
  }

  while (it2 != kvps2.end()) {
    if (it2->value != TOMBSTONE) {
      merged.push_back(*it2);
    }
    ++it2;
  }

  return merged;
}
