#include "db.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <iterator>

#include "exceptions.h"

using namespace std;

bool is_file_exists(string fileName);

bool DB::open(string db_name, int memtable_size, int bp_policy) {
  if (!this->name.empty()) {
    fprintf(stderr, "ERROR: DB %s is already open. Close this DB first.\n",
            this->name.c_str());
    return false;
  }

  int fd_db = ::open(db_name.c_str(), O_RDONLY | O_DIRECTORY, DIR_PERMISSIONS);

  // Check for existing database
  if (fd_db >= 0) {
    // Read metadata
    int fd_metadata = ::openat(fd_db, METADATA_FILE.c_str(),
                               O_RDONLY | O_DIRECT, FILE_PERMISSIONS);
    if (fd_metadata >= 0) {
      Metadata* buff;
      int buff_size = round_up_block_size(sizeof(Metadata));
      int ret = posix_memalign((void**)&buff, BLOCK_SIZE, buff_size);
      if (ret == -1) {
        perror("posix_memalign");
      }
      // Assumption: Metadata struct fits inside a single block in the file
      ret = pread(fd_metadata, buff, buff_size, 0);
      if (ret == -1) {
        perror("pread");
      }
      metadata.memtable_size = buff->memtable_size;
      metadata.next_sst_id = buff->next_sst_id;
      metadata.num_elems = buff->num_elems;
      free(buff);
      ::close(fd_metadata);
    } else {
      fprintf(stderr, "ERROR: Could not open %s file in database %s. %s\n",
              METADATA_FILE.c_str(), db_name.c_str(), strerror(errno));
      ::close(fd_db);
      return false;
    }

    DIR* db_dir = fdopendir(fd_db);
    reopen_ssts_by_age(db_name, db_dir);
    closedir(db_dir);  // This will also close the file descriptor fd_db
  } else {             // Otherwise make new database
    int ret = mkdir(db_name.c_str(), DIR_PERMISSIONS);
    if (ret == -1) {
      fprintf(stderr, "ERROR: Could not create database %s. %s\n",
              db_name.c_str(), strerror(errno));
      return false;
    }
    metadata.memtable_size = memtable_size;
    metadata.next_sst_id = 0;
    metadata.num_elems = 0;
  }

  name = db_name;
  memtable = new Tree(metadata.memtable_size);
  buffer_pool = new BufferPool(DEFAULT_INITIAL_CAPACITY, DEFAULT_MAX_CAPACITY,
                               DEFAULT_EXTEND_THRESHOLD, bp_policy);
  return true;
}

bool DB::close() {
  // Write memtable to disk before closing
  vector<KVPair> kvpairs = memtable->scan(MIN_KEY, MAX_KEY);
  if (kvpairs.size() > 0) {
    string sst_name = get_sst_filename();
    write_sst(kvpairs, sst_name);
    sst_names.push_back(sst_name);
  }
  int fd_db = ::open(name.c_str(), O_RDONLY | O_DIRECTORY, DIR_PERMISSIONS);
  if (fd_db < 0) {
    fprintf(stderr, "ERROR: Could not open database %s. %s\n", name.c_str(),
            strerror(errno));
    return false;
  }
  int fd_metadata = ::openat(fd_db, METADATA_FILE.c_str(),
                             O_CREAT | O_WRONLY | O_DIRECT, FILE_PERMISSIONS);
  if (fd_metadata < 0) {
    fprintf(stderr, "ERROR: Could not open metadata file. %s\n",
            strerror(errno));
    ::close(fd_db);
    return false;
  }
  Metadata* buff;
  int buff_size = round_up_block_size(sizeof(Metadata));
  int ret = posix_memalign((void**)&buff, BLOCK_SIZE, buff_size);
  if (ret == -1) {
    perror("posix_memalign");
  }
  buff->memtable_size = metadata.memtable_size;
  buff->next_sst_id = metadata.next_sst_id;
  buff->num_elems = metadata.num_elems;
  ret = pwrite(fd_metadata, buff, buff_size, 0);
  if (ret == -1) {
    perror("pwrite");
  }
  free(buff);
  ::close(fd_metadata);
  ::close(fd_db);
  this->name = "";
  this->sst_names.clear();
  delete (this->memtable);
  this->buffer_pool->prepare_destroy();
  delete (this->buffer_pool);

  return true;
}

void DB::put(uint64_t key, uint64_t value) {
  if (!memtable->put(key, value)) {
    vector<KVPair> kvpairs = memtable->scan(MIN_KEY, MAX_KEY);
    unsigned int sst_index = 0;
    string sst_name = name + "/" + to_string(sst_index++) + SST_EXTENSION;
    while (is_file_exists(sst_name)) {  // if file exists, merge
      vector<KVPair> old_sst = read_sst(sst_name);
      merge(kvpairs, old_sst);
      sst_name = name + "/" + to_string(sst_index++) + SST_EXTENSION;
    }
    write_sst(kvpairs, sst_name);
    sst_names.push_back(sst_name);
    delete (memtable);
    memtable = new Tree(metadata.memtable_size);
  }
  metadata.num_elems++;
}

void DB::del(uint64_t key) { put(key, TOMBSTONE); }

uint64_t DB::binary_search(vector<KVPair> kvpairs, uint64_t key) {
  int low = 0;
  int high = metadata.memtable_size - 1;
  int mid;

  while (low <= high) {
    mid = (low + high) / 2;
    if (kvpairs[mid].key < key) {
      low = mid + 1;
    } else if (kvpairs[mid].key > key) {
      high = mid - 1;
    } else {
      return kvpairs[mid].value;
    }
  }
  throw KeyException("Key not found in sst");
}

uint64_t DB::get(uint64_t key) {
  try {
    uint64_t value = memtable->get(key);
    return value;
  } catch (const KeyException& e) {
    if (VERBOSE) cerr << e.what() << "\n";
    vector<KVPair> kvpairs;
    for (auto sst = begin(sst_names); sst != end(sst_names); ++sst) {
      try {
        return sst_get(*sst, key, buffer_pool, false);
      } catch (const KeyException& e) {
        if (VERBOSE) cerr << e.what() << "\n";
      }
    }
  }
  throw KeyException("Key not in database");
}

vector<KVPair> DB::scan(uint64_t key1, uint64_t key2) {
  vector<KVPair> output = memtable->scan(key1, key2);

  vector<KVPair> kvpairs;
  for (auto sst = begin(sst_names); sst != end(sst_names); ++sst) {
    kvpairs = sst_scan(*sst, key1, key2);
    output.insert(output.end(), kvpairs.begin(), kvpairs.end());
  }
  return output;
}

string DB::get_sst_filename() {
  return name + "/" + to_string(metadata.next_sst_id++) + SST_EXTENSION;
}

// Put all SST names back in memory when database is reopened
void DB::reopen_ssts_by_age(string dir_name, DIR* dir) {
  struct dirent* ent;
  struct stat statbuf;

  vector<pair<string, struct timespec>>
      sst_name_createtime;  // (name, creation time) pairs

  while ((ent = readdir(dir)) != NULL) {
    string ent_name = string(ent->d_name);
    if (ent_name.length() >= 4 &&
        ent_name.substr(ent_name.length() - 4, ent_name.length()) ==
            SST_EXTENSION) {
      string sst_name = dir_name + "/" + ent_name;
      if (stat(sst_name.c_str(), &statbuf) != 0) {
        perror("stat");
      }
      sst_name_createtime.push_back(make_pair(sst_name, statbuf.st_ctim));
    }
  }

  // Sort by creation time
  sort(begin(sst_name_createtime), end(sst_name_createtime),
       [](const pair<string, struct timespec>& lhs,
          const pair<string, struct timespec>& rhs) {
         struct timespec sst1_ctime = lhs.second;
         struct timespec sst2_ctime = rhs.second;

         return sst1_ctime.tv_sec == sst2_ctime.tv_sec
                    ? sst1_ctime.tv_nsec < sst2_ctime.tv_nsec
                    : sst1_ctime.tv_sec < sst2_ctime.tv_sec;  // Ascending order
       });

  for (auto pair : sst_name_createtime) {
    sst_names.push_back(pair.first);
  }
}

void DB::resize_bp_dir(int new_capacity) { buffer_pool->resize(new_capacity); }

// Taken from https://stackoverflow.com/a/19841704/10254049
bool is_file_exists(string fileName) {
  std::ifstream infile(fileName);
  return infile.good();
}
