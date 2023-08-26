#include "buffer_pool.h"

#include <string>

#include "exceptions.h"

using namespace std;

shared_ptr<BufferPool::LLNode_t> BufferPool::LLNode(string filename,
                                                    int page_number,
                                                    KVPair* page) {
  auto node = make_shared<BufferPool::LLNode_t>();
  node->page = page;
  node->next = NULL;
  node->filename = filename;
  node->page_number = page_number;

  return node;
}

shared_ptr<BufferPool::Bucket_t> BufferPool::Bucket() {
  auto bucket = make_shared<BufferPool::Bucket_t>();
  bucket->head = NULL;
  bucket->ref_count = 0;

  return bucket;
}

BufferPool::BufferPool(int initial_size, int max_size, double extend_threshold,
                       int policy) {
  this->extend_threshold = extend_threshold;
  this->initial_capacity = initial_size;
  this->max_capacity = max_size;
  this->curr_capacity = initial_size;
  this->num_pages = 0;

  directory.resize(initial_size);
  for (int i = 0; i < initial_size; i++) {
    directory[i] = BufferPool::Bucket();
    directory[i]->ref_count = 1;
  }

  if (policy == LRU) {
    this->replacer = make_unique<LRUReplacer>();
  } else if (policy == CLOCK) {
    this->replacer = make_unique<ClockReplacer>();
  } else {
    throw NotImplementedException(
        "Eviction policy not defined - must choose LRU or Clock");
  }
}

KVPair* BufferPool::get(string filename, int page_number) {
  int bucket_num = hash(filename, page_number);
  auto bucket = directory[bucket_num];
  auto curr = bucket->head;

  KVPair* page = NULL;

  while (curr != NULL) {
    if (curr->page_number == page_number && curr->filename == filename) {
      page = curr->page;
      int page_added = this->replacer->record_access(page);
      break;
    }
    curr = curr->next;
  }

  // Opportunistically check if we can rehash (multiple prefixes map to this
  // bucket and there is a chain)
  if (bucket->ref_count > 1 && bucket->head != NULL &&
      bucket->head->next != NULL) {
    BufferPool::rehash(bucket_num);
  }

  return page;
}

void BufferPool::put(string filename, int page_number, KVPair* page) {
  int bucket_num = hash(filename, page_number);
  auto bucket = directory[bucket_num];
  auto new_node = BufferPool::LLNode(filename, page_number, page);

  if (this->curr_capacity == this->max_capacity &&
      this->num_pages >= this->curr_capacity) {
    // TODO: Evict a page
    struct KVPair* page_to_evict = new KVPair;
    if (this->replacer->evict(page_to_evict) == 1) {
      remove_from_bucket(bucket, page_to_evict);
    }
    delete page_to_evict;
  } else {
    this->num_pages++;
  }

  append_to_bucket(bucket, new_node);

  if (this->curr_capacity < this->max_capacity &&
      this->num_pages >= (this->curr_capacity * this->extend_threshold)) {
    BufferPool::extend();
  }
  // Opportunistically check if we can rehash (multiple prefixes map to this
  // bucket and there is a chain)
  if (bucket->ref_count > 1 && bucket->head != NULL &&
      bucket->head->next != NULL) {
    BufferPool::rehash(bucket_num);
  }
}

void BufferPool::remove_from_bucket(shared_ptr<BufferPool::Bucket_t> bucket,
                                    KVPair* evicted_page) {
  auto curr = bucket->head;
  while (1) {
    if (curr->next->page == evicted_page) {
      curr->next = curr->next->next;
      return;
    }
  }
}

void BufferPool::append_to_bucket(shared_ptr<BufferPool::Bucket_t> bucket,
                                  shared_ptr<BufferPool::LLNode_t> new_node) {
  if (bucket->head == NULL) {
    bucket->head = new_node;
  } else {
    auto curr = bucket->head;
    while (curr->next != NULL) {
      curr = curr->next;
    }
    curr->next = new_node;
  }
}

int BufferPool::hash(string filename, int page_number) {
  string key = filename + to_string(page_number);
  size_t hashed_value = std::hash<string>{}(key);
  int least_significant_bits = hashed_value & (curr_capacity - 1);
  return least_significant_bits;
}

void BufferPool::extend() {
  int old_capacity = this->curr_capacity;
  this->curr_capacity = this->curr_capacity << 1;

  directory.resize(this->curr_capacity);
  for (int i = old_capacity; i < this->curr_capacity; i++) {
    directory[i] = directory[i - old_capacity];
    directory[i - old_capacity]->ref_count++;
  }
}

void BufferPool::resize(int new_capacity) {
  if (new_capacity >= this->curr_capacity) {
    this->max_capacity = new_capacity;
  }
  // Shrink in this case
  else {
    // Evict all the pages in the buckets that are going to be removed
    for (int i = this->curr_capacity - 1; i >= new_capacity; i--) {
      auto bucket = directory[i];
      auto curr = bucket->head;

      while (curr != NULL) {
        replacer->evict(curr->page);
        curr = curr->next;
      }
    }
    this->curr_capacity = new_capacity;
    this->max_capacity = new_capacity;
    auto old_directory = this->directory;

    // Make new directory
    this->directory = std::vector<std::shared_ptr<Bucket_t>>(new_capacity);
    for (int i = 0; i < new_capacity; i++) {
      directory[i] = BufferPool::Bucket();
      directory[i]->ref_count = 1;
    }

    // Rehash all the remaining pages
    this->num_pages = 0;
    for (int i = 0; i < new_capacity; i++) {
      auto old_bucket = old_directory[i];
      auto curr = old_bucket->head;
      while (curr != NULL) {
        put(curr->filename, curr->page_number, curr->page);
        curr = curr->next;
      }
    }
  }
}

void BufferPool::rehash(int orig_bucket_num) {
  auto orig_bucket = directory[orig_bucket_num];
  auto curr = orig_bucket->head;

  // Give every prefix that points to this bucket their own bucket
  for (int prefix = 0; prefix < curr_capacity; prefix++) {
    if (directory[prefix] == orig_bucket && prefix != orig_bucket_num) {
      directory[prefix] = BufferPool::Bucket();
      directory[prefix]->ref_count = 1;
    }
  }
  orig_bucket->ref_count = 1;

  // Destroy bucket to rebuild it
  orig_bucket->head = NULL;

  while (curr != NULL) {
    auto next = curr->next;
    curr->next = NULL;
    int rehashed_num = hash(curr->filename, curr->page_number);
    auto rehashed_bucket = directory[rehashed_num];
    append_to_bucket(rehashed_bucket, curr);
    curr = next;
  }
}

BufferPool::~BufferPool() { prepare_destroy(); }

void BufferPool::prepare_destroy() {
  // Put pages in vector to avoid problem freeing a page in multiple buckets
  vector<KVPair*> pages;
  for (auto& bucket : directory) {
    auto curr = bucket->head;
    if (bucket->ref_count > 0) {
      while (curr != NULL) {
        pages.push_back(curr->page);
        curr = curr->next;
      }
      bucket->ref_count = 0;
    }
  }

  for (auto& page : pages) {
    if (page) {
      free(page);
      page = NULL;
    }
  }
}