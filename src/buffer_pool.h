#ifndef _BUFFER_POOL_H
#define _BUFFER_POOL_H

#include <string>
#include <vector>
#include <memory> 
#include "kvpair.h"
#include "replacer.h"
#include "clock_replacer.h"
#include "lru_replacer.h"

const double DEFAULT_EXTEND_THRESHOLD = 0.9;
const int DEFAULT_INITIAL_CAPACITY = 32;
const int DEFAULT_MAX_CAPACITY = 64;

struct BufferPool {
  struct LLNode {
    KVPair* page;
    std::string filename;
    int page_number;
    std::shared_ptr<LLNode> next;
  };
  typedef LLNode LLNode_t;
  std::shared_ptr<LLNode_t> LLNode(std::string, int, KVPair*);

  struct Bucket {
    std::shared_ptr<LLNode_t> head;
    int ref_count; // Number of hash prefixes that point to this bucket (after expansion but before rehash) 
  };
  typedef Bucket Bucket_t;
  std::shared_ptr<Bucket_t> Bucket();

  void rehash(int);

  void extend();
  void resize(int);
  void prepare_destroy();

  void append_to_bucket(std::shared_ptr<Bucket_t>, std::shared_ptr<LLNode_t>);
  void remove_from_bucket(std::shared_ptr<Bucket_t>, KVPair*);

  std::vector<std::shared_ptr<Bucket_t>> directory;
  double extend_threshold; // Extend directory when this threshold is reached
  int initial_capacity; // Capacity should always be power of 2
  int max_capacity;
  int curr_capacity;
  int num_pages;
  std::unique_ptr<Replacer> replacer;
  
  BufferPool(int initial = DEFAULT_INITIAL_CAPACITY, int max = DEFAULT_MAX_CAPACITY,
   double extend_threshold = DEFAULT_EXTEND_THRESHOLD, int policy = CLOCK);
  ~BufferPool();
  KVPair* get(std::string filename, int page_number);
  void put(std::string filename, int page_number, KVPair* page);
  int hash(std::string filename, int page_number);
};

#endif