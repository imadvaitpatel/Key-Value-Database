#ifndef _SST_H
#define _SST_H

#include <stdint.h>
#include <vector>
#include <string>
#include <sys/stat.h>
#include <stdbool.h>

#include "kvpair.h"
#include "buffer_pool.h"

void write_sst(std::vector<KVPair> kv_pairs, std::string filename);
std::vector<KVPair> read_sst(std::string);

uint64_t sst_get(std::string, uint64_t, BufferPool*, bool);
std::vector<KVPair> sst_scan(std::string, uint64_t, uint64_t);

int round_up_block_size(int);
int round_up_page_size(int);

const int FILE_PERMISSIONS = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;
const int DIR_PERMISSIONS = S_IRWXU | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;
const unsigned int BLOCK_SIZE = 512;
const unsigned int B = 256;
const unsigned int NODE_SIZE = B * sizeof(uint64_t);
const unsigned int _PAGE_SIZE = 4096;
const std::string SST_EXTENSION = ".sst";

#endif
