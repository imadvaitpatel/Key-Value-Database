#ifndef _DB_H
#define _DB_H

#include <string.h>
#include <dirent.h>
#include <string>
#include <vector>
#include "avl_tree.h"
#include "sst.h"
#include "buffer_pool.h"

using namespace std;

const int DEFAULT_MEMTABLE_SIZE = 100;
const int VERBOSE = 0;

struct DB {
private:
  uint64_t binary_search(vector<KVPair>, uint64_t);
  void reopen_ssts_by_age(string, DIR*);
  string get_sst_filename();

public:
  // Any DB information that needs to be persisted when DB is closed belongs in the Metadata struct
  struct Metadata {
    int memtable_size;  // Maximum number of KV pairs in a memtable
    int next_sst_id;    // The ID of the next SST that will be created (used in making unique names)
    int num_elems; // Number of elements currently in the DB
  };
  Metadata metadata;
  BufferPool *buffer_pool;
  string name;
  vector<string> sst_names;
  Tree *memtable;
  bool open(
      string db_name, int memtable_size = DEFAULT_MEMTABLE_SIZE,
      int bp_policy = CLOCK);  // Open a new or existing database
  bool close(); // Close the database
  void put(uint64_t key, uint64_t value); // Put a key value pair in the database
  uint64_t get(uint64_t key); // Get the value for key and pass it to ptr
  void del(uint64_t key);
  vector<KVPair> scan(uint64_t key1, uint64_t key2);
  void resize_bp_dir(int);
};

const string METADATA_FILE = "metadata";

#endif
