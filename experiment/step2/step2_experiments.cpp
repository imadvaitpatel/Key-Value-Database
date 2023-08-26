#include <math.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <vector>

#include "../../src/buffer_pool.h"
#include "../../src/db.h"
#include "../../src/sst.h"

#define MAX(a, b) (a > b ? a : b)
#define MIN(a, b) (a < b ? a : b)

uint64_t kv = 1;

uint64_t generate_key_workload(int bp_size, int workload) {
  std::random_device rd;
  std::mt19937_64 gen(rd());
  if (workload == 1) {
    // Generate key that is guaranteed to be found in memory -> better for Clock
    std::uniform_int_distribution<uint64_t> dis(1, bp_size);
    return dis(gen);
  } else {
    // Generate key that is skewed to not be found -> better for LRU
    std::uniform_int_distribution<uint64_t> dis(1, bp_size * 10);
    return dis(gen);
  }
}

std::pair<std::vector<int>, std::vector<int>> experiment_bp(int workload) {
  std::vector<int> buffer_pool_sizes = {
      (int)pow(2, 6),  (int)pow(2, 7),  (int)pow(2, 8),  (int)pow(2, 9),
      (int)pow(2, 10), (int)pow(2, 11), (int)pow(2, 12),
  };

  DB db;
  db.open("step2_db_clock_w" + std::to_string(workload), _PAGE_SIZE, CLOCK);
  int entries_per_page = _PAGE_SIZE / sizeof(KVPair);

  std::cout << "Beginning experiment for throughput of buffer pool using Clock "
               "policy on workload "
            << workload << std::endl;
  std::vector<int> clock_qps;
  for (int max_size : buffer_pool_sizes) {
    int count = 0;
    std::cout << "Buffer pool max number of pages set to " << max_size
              << " and filling database with " << max_size * entries_per_page
              << " entries" << std::endl;
    db.resize_bp_dir(max_size);
    // Fill database and put pages in buffer pool
    while (db.metadata.num_elems < max_size * entries_per_page) {
      db.put(kv, kv);
      db.get(kv);  // Populate buffer pool by calling get
      kv++;
    }
    std::cout << "Testing query throughput on buffer pool with max capacity "
              << max_size * entries_per_page * sizeof(KVPair) << " (in bytes)"
              << std::endl;
    std::chrono::duration<double, std::milli> elapsed =
        std::chrono::duration<double, std::milli>::zero();

    for (int i = 0; i < 10; i++) {
      while (1) {
        int get_key = generate_key_workload(max_size, workload);
        if (elapsed.count() >= 1000) {
          break;
        }
        auto start = std::chrono::high_resolution_clock::now();
        try {
          db.get(get_key);
        } catch (...) {
        }
        auto end = std::chrono::high_resolution_clock::now();
        elapsed += end - start;
        count++;
      }
    }
    clock_qps.push_back(count / 10);
    std::cout << "Number of queries is " << count / 10 << std::endl;
  }

  DB db2;
  db2.open("step2_db_lru_w" + std::to_string(workload), _PAGE_SIZE, LRU);

  kv = 1;
  std::cout << "Beginning experiment for throughput of buffer pool using LRU "
               "policy on workload "
            << workload << std::endl;
  std::vector<int> lru_qps;
  for (int max_size : buffer_pool_sizes) {
    int count = 0;
    std::cout << "Buffer pool max number of pages set to " << max_size
              << " and filling database with " << max_size * entries_per_page
              << " entries" << std::endl;
    db2.resize_bp_dir(max_size);
    // Fill database and put pages in buffer pool
    while (db2.metadata.num_elems < max_size * entries_per_page) {
      db2.put(kv, kv);
      db2.get(kv);  // Populate buffer pool by calling get
      kv++;
    }
    std::cout << "Testing query throughput on buffer pool with max capacity "
              << max_size * entries_per_page * sizeof(KVPair) << " (in bytes)"
              << std::endl;
    std::chrono::duration<double, std::milli> elapsed =
        std::chrono::duration<double, std::milli>::zero();

    for (int i = 0; i < 10; i++) {
      while (1) {
        if (elapsed.count() >= 1000) {
          break;
        }
        int get_key = generate_key_workload(max_size, workload);
        auto start = std::chrono::high_resolution_clock::now();
        try {
          db.get(get_key);
        } catch (...) {
        }
        auto end = std::chrono::high_resolution_clock::now();
        elapsed += end - start;
        count++;
      }
    }
    lru_qps.push_back(count / 10);
    std::cout << "Number of queries is " << count / 10 << std::endl;
  }

  return std::make_pair(clock_qps, lru_qps);
}

int main() {
  std::ofstream myfile;
  myfile.open("step2_results.txt");

  std::cout << "Experiment results will be written to step2_results.txt"
            << std::endl;

  auto buffer_pool_qps_w1 = experiment_bp(1);
  std::vector<int> clock_qps_w1 = buffer_pool_qps_w1.first;
  std::vector<int> lru_qps_w1 = buffer_pool_qps_w1.second;

  for (auto i : clock_qps_w1) {
    myfile << std::to_string(i) << " ";
  }
  myfile.flush();

  for (auto i : lru_qps_w1) {
    myfile << std::to_string(i) << " ";
  }
  myfile.flush();

  auto buffer_pool_qps_w2 = experiment_bp(2);
  std::vector<int> clock_qps_w2 = buffer_pool_qps_w2.first;
  std::vector<int> lru_qps_w2 = buffer_pool_qps_w1.second;

  for (auto i : clock_qps_w2) {
    myfile << std::to_string(i) << " ";
  }
  myfile.flush();

  for (auto i : lru_qps_w2) {
    myfile << std::to_string(i) << " ";
  }
  myfile.flush();

  myfile.close();
}