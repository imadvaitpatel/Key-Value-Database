#ifndef _KV_PAIR_H
#define _KV_PAIR_H

#define MIN_KEY 0
#define MAX_KEY 0xffffffffffffffff
#define TOMBSTONE 0xfffffffffffffffe

#include <vector>
#include <cstdint>

class KVPair {
public:
  uint64_t key;
  uint64_t value;

  bool operator==(const KVPair& other) const {
    return (key == other.key) && (value == other.value);
  }
};

std::vector<KVPair> merge(std::vector<KVPair>, std::vector<KVPair>);

// Use this reserved pair for indicating NULL and end of block
const struct KVPair NULL_PAIR = {
  MAX_KEY,
  MAX_KEY
};

#endif
