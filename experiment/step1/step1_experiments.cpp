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

#include "../../src/db.h"

#define MAX(a, b) (a > b ? a : b)
#define MIN(a, b) (a < b ? a : b)

uint64_t generate_random() {
  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<uint64_t> dis;
  return dis(gen);
}

void fill_db(DB *db, long volume) {
  // Fill database with data
  uint64_t x;
  while ((*db).metadata.num_elems < volume) {
    x = generate_random();
    (*db).put(x, x);
  }
}

std::vector<int> experiment_get() {
  std::cout << "Beginning experiment for throughput of get()" << std::endl;
  std::vector<int> volumes = {(int)pow(2, 10) / (int)sizeof(KVPair),
                              (int)pow(2, 16) / (int)sizeof(KVPair),
                              (int)pow(2, 20) / (int)sizeof(KVPair),
                              (int)pow(2, 22) / (int)sizeof(KVPair),
                              (int)pow(2, 24) / (int)sizeof(KVPair),
                              (int)pow(2, 26) / (int)sizeof(KVPair),
                              (int)pow(2, 27) / (int)sizeof(KVPair),
                              (int)pow(2, 28) / (int)sizeof(KVPair)};
  std::vector<int> qps;

  uint64_t x;
  int count;

  DB db;
  db.open("step1_db_get", (int)pow(2, 20));

  for (int vol : volumes) {
    count = 0;
    std::cout << "Filling database with " << vol * sizeof(KVPair) << " bytes..."
              << std::endl;
    fill_db(&db, vol);
    std::cout << "Testing throughput with data size " << vol * sizeof(KVPair)
              << " bytes..." << std::endl;
    for (int i = 0; i < 10; i++) {
      auto start = std::chrono::high_resolution_clock::now();
      while (1) {
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> elapsed = end - start;
        if (elapsed.count() >= 1000) {
          break;
        }
        x = generate_random();
        try {
          db.get(x);
        } catch (...) {
        }
        count++;
      }
    }
    qps.push_back(count / 10);
  }
  db.close();
  return qps;
}

std::vector<int> experiment_put() {
  std::cout << "Beginning experiment for throughput of put()" << std::endl;
  std::vector<int> volumes = {(int)pow(2, 10) / (int)sizeof(KVPair),
                              (int)pow(2, 16) / (int)sizeof(KVPair),
                              (int)pow(2, 20) / (int)sizeof(KVPair),
                              (int)pow(2, 22) / (int)sizeof(KVPair),
                              (int)pow(2, 24) / (int)sizeof(KVPair),
                              (int)pow(2, 26) / (int)sizeof(KVPair),
                              (int)pow(2, 27) / (int)sizeof(KVPair),
                              (int)pow(2, 28) / (int)sizeof(KVPair)};
  std::vector<int> qps;

  uint64_t x;
  int count;

  for (long vol : volumes) {
    count = 0;
    std::cout << "Creating and filling database with " << vol * sizeof(KVPair)
              << " bytes..." << std::endl;
    for (int i = 0; i < 10; i++) {
      DB db;
      db.open("step1_db_put" + std::to_string(vol), (int)pow(2, 20));
      fill_db(&db, vol);
      std::cout << "Testing throughput with data size " << vol * sizeof(KVPair)
                << " bytes..." << std::endl;
      auto start = std::chrono::high_resolution_clock::now();
      while (1) {
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> elapsed = end - start;
        if (elapsed.count() >= 1000) {
          break;
        }
        x = generate_random();
        db.put(x, x);
        count++;
      }
      db.close();
    }
    qps.push_back(count / 10);
  }

  return qps;
}

std::vector<int> experiment_scan() {
  std::cout << "Beginning experiment for throughput of scan()" << std::endl;

  DB db;
  db.open("step1_db_scan", (int)pow(2, 20));

  std::vector<int> volumes = {(int)pow(2, 10) / (int)sizeof(KVPair),
                              (int)pow(2, 16) / (int)sizeof(KVPair),
                              (int)pow(2, 20) / (int)sizeof(KVPair),
                              (int)pow(2, 22) / (int)sizeof(KVPair),
                              (int)pow(2, 24) / (int)sizeof(KVPair),
                              (int)pow(2, 26) / (int)sizeof(KVPair),
                              (int)pow(2, 27) / (int)sizeof(KVPair),
                              (int)pow(2, 28) / (int)sizeof(KVPair)};
  std::vector<int> qps;

  int count;
  uint64_t x, y;

  for (int vol : volumes) {
    count = 0;
    std::cout << "Filling database with " << vol * sizeof(KVPair) << " bytes..."
              << std::endl;
    fill_db(&db, vol);
    std::cout << "Testing throughput with data size " << vol * sizeof(KVPair)
              << " bytes..." << std::endl;
    for (int i = 0; i < 10; i++) {
      auto start = std::chrono::high_resolution_clock::now();
      while (1) {
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> elapsed = end - start;
        if (elapsed.count() >= 1000) {
          break;
        }
        x = generate_random();
        y = generate_random();
        db.scan(MIN(x, y), MAX(x, y));
        count++;
      }
    }
    qps.push_back(count / 10);
  }

  db.close();

  return qps;
}

int main() {
  ofstream myfile;
  myfile.open("step1_results.txt");

  std::cout << "Experiment results will be written to step1_results.txt"
            << std::endl;

  std::vector<int> qps_get;
  qps_get = experiment_get();
  for (auto i : qps_get) {
    myfile << std::to_string(i) << " ";
  }
  myfile.flush();

  std::vector<int> qps_scan;
  qps_scan = experiment_scan();
  for (auto i : qps_scan) {
    myfile << std::to_string(i) << " ";
  }
  myfile.flush();

  std::vector<int> qps_put;
  qps_put = experiment_put();
  for (auto i : qps_put) {
    myfile << std::to_string(i) << " ";
  }
  myfile.flush();

  myfile.close();
}
