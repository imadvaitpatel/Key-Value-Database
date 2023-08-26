#ifndef _LRU_REPLACER_H
#define _LRU_REPLACER_H

#include <string>
#include <vector>
#include <memory> 
#include "kvpair.h"
#include "replacer.h"

struct LRUReplacer : Replacer {
  struct Node {
      KVPair *page;
      std::shared_ptr<Node> prev;
      std::shared_ptr<Node> next;
  };
  typedef Node Node_t;
  
  std::shared_ptr<Node_t> head;
  std::shared_ptr<Node_t> tail;
  int size;

  std::shared_ptr<Node_t> Node(KVPair *page);
  LRUReplacer();
  int evict(KVPair *page);
  int record_access(KVPair *page);

};

#endif