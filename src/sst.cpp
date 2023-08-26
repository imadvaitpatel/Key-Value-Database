#include "sst.h"

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cmath>
#include <string>

#include "exceptions.h"

using namespace std;

int kv_pairs_to_btree(void **, vector<KVPair> &);
bool read_sst_page(std::string, KVPair *, int);

int find_lower_bound_page(int, uint64_t, KVPair **);
int find_key_page(int, std::string, uint64_t, KVPair **, BufferPool *);
int find_key_page_btree(int, std::string, uint64_t, KVPair **, BufferPool *);
uint64_t get_in_page(KVPair *, uint64_t, int);

int get_num_pages(int);
int page_num_entries(KVPair *, bool);
int round_up(int, int);

void write_sst(vector<KVPair> kv_pairs, string filename) {
  int fd =
      open(filename.c_str(), O_CREAT | O_WRONLY | O_DIRECT, FILE_PERMISSIONS);

  if (fd < 0) {
    perror("open");
  }

  void *buff;
  int buff_size = kv_pairs_to_btree(&buff, kv_pairs);

  int ret = pwrite(fd, buff, buff_size, 0);
  if (ret == -1) {
    perror("pwrite");
  }
  free(buff);
  close(fd);
}

int kv_pairs_to_btree(void **buff, vector<KVPair> &kv_pairs) {
  // Add special KV Pair at the end to indicate the end of the written block in
  // the file
  kv_pairs.push_back(NULL_PAIR);

  int buff_size =
      round_up_page_size(sizeof(KVPair) * kv_pairs.size() + NODE_SIZE);
  int ret = posix_memalign(buff, BLOCK_SIZE, buff_size);
  if (ret != 0) {
    perror("posix_memalign");
  }

  uint64_t *key_buff = (uint64_t *)*buff;
  KVPair *kvpair_buff = (KVPair *)(*buff + NODE_SIZE);

  key_buff[0] = kv_pairs.size() / B;
  for (int i = 0, j = 1; i < kv_pairs.size(); i++) {
    kvpair_buff[i].key = kv_pairs[i].key;
    kvpair_buff[i].value = kv_pairs[i].value;

    if (i % B == B - 1) {
      key_buff[j] = kv_pairs[i].key;
      j++;
    }
  }

  return buff_size;
}

vector<KVPair> read_sst(string filename) {
  vector<KVPair> kv_pairs;
  int fd = open(filename.c_str(), O_RDONLY | O_DIRECT, FILE_PERMISSIONS);

  if (fd < 0) {
    perror("open");
  }

  void *buff;
  int buff_size = round_up_page_size(sizeof(KVPair));
  int ret = posix_memalign(&buff, BLOCK_SIZE, buff_size);
  if (ret != 0) {
    perror("posix_memalign");
  }
  KVPair *kvpair_buff = (KVPair *)buff;

  int i = 0;
  int bytes;
  // Read file block-by-block
  while ((bytes = pread(fd, kvpair_buff, buff_size,
                        NODE_SIZE + i * buff_size)) > 0) {
    if (bytes == -1) {
      perror("pread");
    }
    int pairs_in_block = bytes / sizeof(KVPair);
    for (int j = 0; j < pairs_in_block; j++) {
      // Check if we reached the end of the block
      if (kvpair_buff[j].key == NULL_PAIR.key &&
          kvpair_buff[j].value == NULL_PAIR.value) {
        break;
      }
      KVPair kv_pair;
      kv_pair.key = kvpair_buff[j].key;
      kv_pair.value = kvpair_buff[j].value;
      kv_pairs.push_back(kv_pair);
    }
    i++;
  }
  free(kvpair_buff);
  close(fd);

  return kv_pairs;
}

int read_sst_page(int fd, int page_index, KVPair **buffer) {
  int bytes;
  if ((bytes = pread(fd, (void *)*buffer, _PAGE_SIZE,
                     NODE_SIZE + page_index * _PAGE_SIZE)) == -1) {
    perror("pread");
  }
  return bytes;
}

vector<KVPair> sst_scan(string filename, uint64_t key1, uint64_t key2) {
  int fd = open(filename.c_str(), O_RDONLY | O_DIRECT, FILE_PERMISSIONS);
  if (fd < 0) {
    perror("open");
  }

  int buff_size = round_up_page_size(sizeof(KVPair));
  KVPair *buff;
  if (posix_memalign((void **)&buff, BLOCK_SIZE, buff_size) != 0) {
    perror("posix_memalign");
  }

  vector<KVPair> kvpairs;
  // Find the page containing the smallest key greater or equal to key1
  int lower_page_index = find_lower_bound_page(fd, key1, &buff);

  if (lower_page_index == -1) {
    free(buff);
    close(fd);
    return kvpairs;
  }

  // Walk until we encounter a key outside of the range
  int num_pages = get_num_pages(fd);
  for (int i = lower_page_index; i < num_pages; i++) {
    if (read_sst_page(fd, i, &buff) > 0) {
      int num_entries = page_num_entries(buff, i == num_pages - 1);
      for (int j = 0; j < num_entries; j++) {
        if (key1 <= buff[j].key && buff[j].key <= key2 &&
            buff[j].value != TOMBSTONE) {
          kvpairs.push_back(buff[j]);
        } else if (buff[j].key > key2) {
          goto end;
        }
      }
    }
  }

end:
  free(buff);
  close(fd);
  return kvpairs;
}

// Return the index of the page containing the first key greater or equal to the
// given key The buffer will also be filled with the contents of the page.
// Return -1 if all keys in the page are smaller than the given key.
int find_lower_bound_page(int fd, uint64_t key, KVPair **buff) {
  int num_pages = get_num_pages(fd);
  int low = 0;
  int high = num_pages;  // Not num_pages - 1
  int mid;

  while (low < high) {
    mid = (high + low) / 2;
    if (read_sst_page(fd, mid, buff) > 0) {
      int num_entries = page_num_entries(*buff, mid == num_pages - 1);
      if (key <= (*buff)[num_entries - 1].key) {
        high = mid;
      } else {
        low = mid + 1;
      }
    } else {
      high = mid;
    }
  }

  return mid == num_pages ? -1 : mid;
}

uint64_t sst_get(string filename, uint64_t key, BufferPool *bp,
                 bool use_btree) {
  int fd = open(filename.c_str(), O_RDONLY | O_DIRECT, FILE_PERMISSIONS);
  if (fd < 0) {
    perror("open");
  }

  int buff_size = round_up_page_size(sizeof(KVPair));
  KVPair *buff;
  if (posix_memalign((void **)&buff, BLOCK_SIZE, buff_size) != 0) {
    perror("posix_memalign");
  }

  int page_index = use_btree ? find_key_page_btree(fd, filename, key, &buff, bp)
                             : find_key_page(fd, filename, key, &buff, bp);

  if (page_index == -1) {
    // free(buff); Don't free this because it points to a page in the buffer
    // pool
    close(fd);
    throw KeyException("Key not found in sst");
  }

  bool is_last_page = page_index == get_num_pages(fd) - 1 ? true : false;
  int num_entries = page_num_entries(buff, is_last_page);
  uint64_t value = get_in_page(buff, key, num_entries);
  // free(buff); Don't free this because it points to a page in the buffer pool
  close(fd);
  return value;
}

// Find the value of a key in a page buffer. This function assumes the key
// exists in the page.
uint64_t get_in_page(KVPair *page, uint64_t key, int num_entries) {
  int res = NULL_PAIR.value;
  int low = 0;
  int high = num_entries - 1;
  int mid;

  while (low <= high) {
    int mid = (high + low) / 2;
    if (page[mid].key == key && page[mid].value == TOMBSTONE) {
      break;
    } else if (page[mid].key == key) {
      return page[mid].value;
    } else if (page[mid].key > key) {
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  }
  throw KeyException("Key not found in page");
}

// Return the index of the page containing a key. The buffer will also be filled
// with the contents of the page.
int find_key_page(int fd, string filename, uint64_t key, KVPair **buff,
                  BufferPool *bp) {
  int num_pages = get_num_pages(fd);

  int low = 0;
  int high = num_pages - 1;
  int mid;

  while (low <= high) {
    mid = (high + low) / 2;
    KVPair *in_memory = bp->get(filename, mid);
    int bytes = 0;
    if (in_memory != NULL) {
      *buff = in_memory;
      bytes = _PAGE_SIZE;
    } else {
      bytes = read_sst_page(fd, mid, buff);
      if (bytes > 0) {
        KVPair *buffer_pool_page;
        if (posix_memalign((void **)&buffer_pool_page, BLOCK_SIZE, bytes) !=
            0) {
          perror("posix_memalign");
        }
        memcpy(buffer_pool_page, *buff, bytes);
        bp->put(filename, mid, buffer_pool_page);
      }
    }
    if (bytes > 0) {
      int num_entries = page_num_entries(*buff, mid == num_pages - 1);
      if (key < (*buff)[0].key) {
        high = mid - 1;
      } else if (key > (*buff)[num_entries - 1].key) {
        low = mid + 1;
      } else {
        return mid;
      }
    } else {
      high = mid - 1;
    }
  }

  return -1;
}

// Return the index of the page containing a key. The buffer will also be filled
// with the contents of the page.
int find_key_page_btree(int fd, string filename, uint64_t key,
                        KVPair **kvpair_buff, BufferPool *bp) {
  uint64_t *key_buff;
  if (posix_memalign((void **)&key_buff, BLOCK_SIZE, BLOCK_SIZE) != 0) {
    perror("posix_memalign");
  }
  if (pread(fd, (void *)key_buff, _PAGE_SIZE, 0) == -1) {
    perror("pread");
  }

  int low = 0;
  int high = key_buff[0];
  int mid;

  while (low <= high) {
    mid = (high + low) / 2;
    if (key_buff[mid] > key) {
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  }

  read_sst_page(fd, mid, kvpair_buff);
  int num_entries = page_num_entries(*kvpair_buff, mid == key_buff[0]);

  low = 0;
  high = num_entries - 1;
  mid;

  while (low <= high) {
    mid = (high + low) / 2;
    if ((*kvpair_buff)[mid].key == key) {
      return mid;
    } else if ((*kvpair_buff)[mid].key > key) {
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  }

  return -1;
}

int page_num_entries(KVPair *page, bool is_last_page) {
  if (!is_last_page) {
    // If this is not the last page, it should be full.
    return _PAGE_SIZE / sizeof(KVPair);
  }

  int i = 0;
  while (i < _PAGE_SIZE / sizeof(KVPair) &&
         (page[i].key != NULL_PAIR.key || page[i].value != NULL_PAIR.value)) {
    i++;
  }
  return i;
}

// Return the number of pages in an open file given its file descriptor
int get_num_pages(int fd) {
  double size_bytes = lseek(fd, 0, SEEK_END);
  if (size_bytes < 0) {
    perror("lseek");
  }
  if (lseek(fd, 0, SEEK_SET) < 0) {
    perror("lseek");
  }
  return ceil(size_bytes / _PAGE_SIZE);
}

int round_up_block_size(int n) { return round_up(n, BLOCK_SIZE); }

int round_up_page_size(int n) { return round_up(n, _PAGE_SIZE); }

// Round up a number n to the nearest multiple of another number m
int round_up(int n, int m) {
  if (m == 0) {
    return n;
  }
  int r = n % m;
  return r == 0 ? r : n + m - r;
}
