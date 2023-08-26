#include "clock_replacer.h"

#include <string>

using namespace std;

shared_ptr<ClockReplacer::Node_t> ClockReplacer::Node(KVPair *page) {
  auto node = make_shared<ClockReplacer::Node_t>();
  node->page = page;
  node->next = NULL;
  node->prev = NULL;
  node->access_bit = 0;
  return node;
}

ClockReplacer::ClockReplacer() {
  this->head = NULL;
  this->size = 0;
}

int ClockReplacer::evict(KVPair *page) {
  if (this->size == 0) {
    return 0;
  }
  bool evicted = false;
  auto curr = this->head;
  while (!evicted) {
    if (curr->access_bit == 0) {
      evicted = true;
      this->size -= 1;
      page = curr->page;
      if (curr == this->head) {
        this->head = curr->next;
      }
      if (curr->next != NULL) {
        curr->next->prev = curr->prev;
      }
      if (curr->prev != NULL) {
        curr->prev->next = curr->next;
      }
    } else {
      curr = curr->next;
      if (curr == NULL) {
        curr = this->head;
      }
    }
  }
  return 1;
}

int ClockReplacer::record_access(KVPair *page) {
  if (this->size == 0) {
    auto new_node = ClockReplacer::Node(page);
    new_node->next = NULL;
    new_node->prev = NULL;
    this->head = new_node;
    this->size += 1;
    return 1;
  }

  auto curr = this->head;
  int i = 0;
  while (i < this->size) {
    if (curr->page == page) {
      curr->access_bit = 1;
      return 0;
    }
    curr = curr->next;
    i += 1;
  }

  auto new_node = ClockReplacer::Node(page);
  new_node->prev = NULL;
  new_node->next = this->head;
  new_node->next->prev = new_node;
  this->head = new_node;
  this->size += 1;
  return 1;
}
