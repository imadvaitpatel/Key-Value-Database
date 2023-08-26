#ifndef _CLOCK_REPLACER_H
#define _CLOCK_REPLACER_H

#include <string>
#include <vector>
#include <memory>
#include "replacer.h"
#include "kvpair.h"

struct ClockReplacer : Replacer {
  struct Node {
      KVPair *page;
      int access_bit;
      std::shared_ptr<Node> prev;
      std::shared_ptr<Node> next;
  };
  typedef Node Node_t;
  
  std::shared_ptr<Node_t> head;
  int size;

  std::shared_ptr<Node_t> Node(KVPair *page);
  ClockReplacer();
  int evict(KVPair *page);
  int record_access(KVPair *page);
};

#endif