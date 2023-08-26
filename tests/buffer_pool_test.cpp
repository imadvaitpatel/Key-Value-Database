#include "../src/buffer_pool.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

#include "../src/kvpair.h"
#include "../src/sst.h"

using namespace std;

void compare_pages(KVPair* p1, KVPair* p2) {
  assert(p1[0].key == p2[0].key);
  assert(p1[0].value == p2[0].value);
}

KVPair* dummy_page(uint64_t key, uint64_t value) {
  KVPair* page = (KVPair*)malloc(_PAGE_SIZE);
  KVPair pair = {.key = key, .value = value};
  page[0] = pair;
  return page;
}

void test_put_get() {
  int capacity = 256;
  BufferPool bp = BufferPool(capacity, capacity * 2);

  vector<KVPair*> pages;
  for (int i = 0; i < capacity; i++) {
    pages.push_back(dummy_page(i + 1, (i + 1) * 10));
    bp.put("sst" + to_string(i), i, pages[i]);
  }

  for (int i = 0; i < capacity; i++) {
    compare_pages(bp.get("sst" + to_string(i), i), pages[i]);
  }
}

void test_rehash() {
  int capacity = 256;
  int max_capacity = capacity * 2;
  BufferPool bp = BufferPool(capacity, max_capacity, 1.0);

  int old_prefix = bp.hash("", 443);  // We keep track of this bucket
  int items_before_rehash = 0;

  // Insert random data
  vector<KVPair*> pages;
  for (int i = 0; i < capacity / 2; i++) {
    int key = i;
    pages.push_back(dummy_page(i + 1, (i + 1) * 10));
    bp.put("", key, pages[i]);
    if (bp.hash("", key) == old_prefix) {
      items_before_rehash++;
    }
  }

  // Insert a bunch of pages that map to our bucket, causing long chain
  // Insert enough entries to force extending and rehashing
  int key = 442;
  for (int i = capacity / 2; i < capacity; i++) {
    pages.push_back(dummy_page(i + 1, (i + 1) * 10));
    while (true) {
      key++;
      if (bp.hash("", key) == old_prefix) {
        break;
      }
    }
    bp.put("", key, pages[i]);
    items_before_rehash++;
  }

  // Buffer pool should have extended and also rehashed bucket
  assert(bp.num_pages == capacity);
  assert(bp.curr_capacity == max_capacity);
  assert(bp.directory[old_prefix]->ref_count == 1);

  // The number of items in the new bucket plus our bucket after rehashing
  // should equal the number of items in our bucket before rehashing
  int our_bucket_after_rehash = 0;
  auto curr = bp.directory[old_prefix]->head;
  while (curr != NULL) {
    our_bucket_after_rehash++;
    curr = curr->next;
  }
  assert(our_bucket_after_rehash > 0);

  int new_prefix = old_prefix + capacity;
  int new_rehashed_bucket = 0;
  curr = bp.directory[new_prefix]->head;
  while (curr != NULL) {
    new_rehashed_bucket++;
    curr = curr->next;
  }
  assert(new_rehashed_bucket > 0);
  assert(our_bucket_after_rehash + new_rehashed_bucket == items_before_rehash);
}

void test_extend() {
  int max_capacity = 512;
  int capacity = max_capacity / 8;
  BufferPool bp = BufferPool(capacity, max_capacity, 1.0);

  vector<KVPair*> pages;
  for (int i = 0; i < max_capacity; i++) {
    pages.push_back(dummy_page(i + 1, (i + 1) * 10));
    bp.put("", i, pages[i]);
  }

  for (int i = 0; i < max_capacity; i++) {
    compare_pages(bp.get("", i), pages[i]);
  }
}

void test_shrink() {
  int capacity = 256;
  BufferPool bp = BufferPool(capacity, capacity * 2);

  vector<KVPair*> pages;
  for (int i = 0; i < capacity; i++) {
    pages.push_back(dummy_page(i + 1, (i + 1) * 10));
    bp.put("sst" + to_string(i), i, pages[i]);
  }

  int resize_capacity = 128;
  bp.resize(resize_capacity);
  assert(bp.curr_capacity == 128);

  // Check that the remaining number of pages all exist
  int num_pages = 0;
  for (int i = 0; i < bp.curr_capacity; i++) {
    auto curr = bp.directory[i]->head;
    while (curr != NULL) {
      num_pages++;
      curr = curr->next;
    }
  }
  assert(num_pages == bp.num_pages);
}

void test_lru_access() {
  LRUReplacer lru = LRUReplacer();
  vector<KVPair*> pages;
  int n = 5;

  for (int i = 0; i < n; i++) {
    pages.push_back(dummy_page(i + 1, (i + 1) * 10));
  }

  // add new elements
  for (int i = 0; i < 5; i++) {
    lru.record_access(pages[i]);
  }

  assert(lru.size == n);

  // record access to previously stored element
  for (int i = 0; i < 5; i++) {
    lru.record_access(pages[i]);
  }

  assert(lru.size == n);
}

void test_lru_evict() {
  LRUReplacer lru = LRUReplacer();
  vector<KVPair*> pages;
  int n = 5;

  for (int i = 0; i < n; i++) {
    pages.push_back(dummy_page(i + 1, (i + 1) * 10));
  }

  // add new elements
  for (int i = 0; i < n; i++) {
    lru.record_access(pages[i]);
  }

  assert(lru.size == n);

  KVPair* new_page = dummy_page(10, 100);

  for (int i = 0; i < n; i++) {
    lru.evict(new_page);
  }

  assert(lru.size == 0);
}

void test_clock_access() {
  ClockReplacer cr = ClockReplacer();
  vector<KVPair*> pages;
  int n = 5;

  for (int i = 0; i < n; i++) {
    pages.push_back(dummy_page(i + 1, (i + 1) * 10));
  }

  // add new elements
  for (int i = 0; i < 5; i++) {
    cr.record_access(pages[i]);
  }

  assert(cr.size == n);

  // record access to previously stored element
  for (int i = 0; i < 5; i++) {
    cr.record_access(pages[i]);
  }

  assert(cr.size == n);
}

void test_clock_evict() {
  ClockReplacer cr = ClockReplacer();
  vector<KVPair*> pages;
  int n = 5;

  for (int i = 0; i < n; i++) {
    pages.push_back(dummy_page(i + 1, (i + 1) * 10));
  }

  // add new elements
  for (int i = 0; i < n; i++) {
    cr.record_access(pages[i]);
  }

  assert(cr.size == n);

  KVPair* new_page = dummy_page(10, 100);

  for (int i = 0; i < n; i++) {
    cr.evict(new_page);
  }

  assert(cr.size == 0);
}

int main() {
  test_put_get();
  test_rehash();
  test_extend();
  test_shrink();
  cout << "Buffer pool tests passed!\n";
  test_lru_access();
  test_lru_evict();
  test_clock_access();
  test_clock_evict();
  cout << "Eviction tests passed!\n";
  return 0;
}
