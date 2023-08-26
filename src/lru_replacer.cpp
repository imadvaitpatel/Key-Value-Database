#include "lru_replacer.h"

#include <string>

using namespace std;

shared_ptr<LRUReplacer::Node_t> LRUReplacer::Node(KVPair* page) {
  auto node = make_shared<LRUReplacer::Node_t>();
  node->page = page;
  node->next = NULL;
  node->prev = NULL;
  return node;
}

LRUReplacer::LRUReplacer() {
  this->head = NULL;
  this->tail = NULL;
  this->size = 0;
}

int LRUReplacer::evict(KVPair* page) {
  if (this->size == 0) {
    return 0;
  }
  this->size -= 1;
  page = this->head->page;
  this->head = this->head->next;
  if (this->size != 0) {
    this->head->prev = NULL;
  }
  return 1;
}

int LRUReplacer::record_access(KVPair* page) {
  if (this->size == 0) {
    auto new_node = LRUReplacer::Node(page);
    this->head = new_node;
    this->tail = new_node;
    this->size += 1;
    return 1;
  }

  auto curr = this->head;
  while (curr != NULL) {
    if (curr->page == page) {
      if (curr != this->tail) {
        if (curr == this->head) {
          this->head = curr->next;
        } else {
          curr->prev->next = curr->next;
        }
        curr->next->prev = curr->prev;
        curr->prev = this->tail;
        curr->next = NULL;
        this->tail->next = curr;
        this->tail = curr;
      }
      break;
    }
    curr = curr->next;
  }

  if (curr == NULL) {
    auto new_node = LRUReplacer::Node(page);
    new_node->prev = this->tail;
    this->tail->next = new_node;
    this->tail = new_node;
    this->size += 1;
    return 1;
  }
  return 0;
}
