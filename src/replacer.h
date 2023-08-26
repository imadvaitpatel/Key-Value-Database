#ifndef _REPLACER_H
#define _REPLACER_H

#include "kvpair.h"

#define CLOCK 0
#define LRU 1

struct Replacer {
  virtual int evict(KVPair *page) = 0;
  virtual int record_access(KVPair *page) = 0;
};

#endif