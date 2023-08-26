#include "avl_tree.h"

#include <algorithm>
#include <iostream>
#include <stack>

#include "exceptions.h"

using namespace std;

Tree::Node_t Tree::NIL = {0, 0, &Tree::NIL, &Tree::NIL, -1};

Tree::Node_t *Tree::Node(uint64_t key, uint64_t value) {
  Tree::Node_t *node = new Tree::Node_t;
  node->key = key;
  node->value = value;
  node->left = &NIL;
  node->right = &NIL;
  node->height = 0;
  return node;
}

Tree::Node_t *Tree::insert(Tree::Node_t *root, uint64_t key, uint64_t value) {
  if (root == &NIL) {
    root = Tree::Node(key, value);
  } else if (key < root->key) {
    root->left = Tree::insert(root->left, key, value);
    root = rebalance_right(root);
  } else if (key > root->key) {
    root->right = Tree::insert(root->right, key, value);
    root = rebalance_left(root);
  } else {
    root->value = value;
  }
  return root;
}

Tree::Node_t *Tree::rebalance_left(Tree::Node_t *root) {
  root->height = 1 + max(root->left->height, root->right->height);
  if (root->right->height > root->left->height + 1) {
    if (root->right->left->height > root->right->right->height) {
      root->right = rotate_right(root->right);
    }
    root = rotate_left(root);
  }
  return root;
}

Tree::Node_t *Tree::rebalance_right(Tree::Node_t *root) {
  root->height = 1 + max(root->left->height, root->right->height);
  if (root->left->height > root->right->height + 1) {
    if (root->left->right->height > root->left->left->height) {
      root->left = rotate_left(root->left);
    }
    root = rotate_right(root);
  }
  return root;
}

Tree::Node_t *Tree::rotate_left(Tree::Node_t *parent) {
  Node_t *child = parent->right;
  parent->right = child->left;
  child->left = parent;

  parent->height = 1 + max(parent->left->height, parent->right->height);
  child->height = 1 + max(child->left->height, child->right->height);

  return child;
}

Tree::Node_t *Tree::rotate_right(Tree::Node_t *parent) {
  Node_t *child = parent->left;
  parent->left = child->right;
  child->right = parent;

  parent->height = 1 + max(parent->left->height, parent->right->height);
  child->height = 1 + max(child->left->height, child->right->height);

  return child;
}

Tree::Tree(unsigned int memtable_size) {
  root = &NIL;
  ttl = memtable_size;
}

Tree::~Tree() { DestructorRec(root); }

void Tree::DestructorRec(Node_t *node) {
  if (node != &NIL) {
    DestructorRec(node->left);
    DestructorRec(node->right);
    delete node;
  }
}

bool Tree::put(uint64_t key, uint64_t value) {
  root = insert(root, key, value);
  return --ttl > 0;
}

uint64_t Tree::get(uint64_t key) {
  Node_t *node = root;
  while (node != &NIL) {
    if (key < node->key) {
      node = node->left;
    } else if (key > node->key) {
      node = node->right;
    } else if (node->value == TOMBSTONE) {
      break;
    } else {
      return node->value;
    }
  }
  throw KeyException("Key not found in tree");
}

vector<KVPair> Tree::scan(uint64_t lower, uint64_t upper) {
  vector<KVPair> output;
  if (root == &NIL) return output;

  stack<Node_t *> s;
  Node_t *curr = root;
  while (curr != &NIL || !s.empty()) {
    while (curr != &NIL && curr->key >= lower) {
      if (curr->key <= upper) {
        s.push(curr);
      }
      curr = curr->left;
    }
    if (s.empty()) break;
    curr = s.top();
    s.pop();
    if (curr->value != TOMBSTONE) {
      output.push_back({.key = curr->key, .value = curr->value});
    }
    curr = curr->right;
  }
  return output;
}
