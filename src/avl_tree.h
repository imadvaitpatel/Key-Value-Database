#ifndef _TREE_H
#define _TREE_H

#include <cstdint>
#include <vector>
#include "kvpair.h"

using namespace std;

class Tree {
private:
  struct Node {
    uint64_t key;
    uint64_t value;
    Node *left;
    Node *right;
    int height;
  };
  typedef Node Node_t;

  Node_t *root;
  unsigned int ttl;

  Node_t *Node(uint64_t, uint64_t);
  Node_t *insert(Node_t*, uint64_t, uint64_t);
  Node_t *rebalance_left(Node_t*);
  Node_t *rebalance_right(Node_t*);
  Node_t *rotate_left(Node_t*);
  Node_t *rotate_right(Node_t*);

  static Node_t NIL;

  void DestructorRec(Node_t*);

public:
  Tree(unsigned int);
  ~Tree();
  bool put(uint64_t, uint64_t); // returns false when ttl reaches 0
  uint64_t get(uint64_t);
  vector<KVPair> scan(uint64_t, uint64_t);
};


#endif
