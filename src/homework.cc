#include "bench.h"

#include <cstring>
#include <iostream>

namespace sqliteBench {
  
  // #1. Write a code for setting the journal mode in the SQLite database engine
  // [Requirement]
  // (1) This function set jounral mode using "FLAGS_journal_mode" extern variable (user input)
  // (2) This function check journal mode; if user try to set wrong journal mode then return -2
  // (3) This function returns status (int data type) of sqlite API function
  int Benchmark::benchmark_setJournalMode() {

    bool isDelete = (std::strcmp(FLAGS_journal_mode, "DELETE") == 0 || 
                     std::strcmp(FLAGS_journal_mode, "delete") == 0);
    bool isTrunc = (std::strcmp(FLAGS_journal_mode, "TRUNCATE") == 0 ||
                     std::strcmp(FLAGS_journal_mode, "truncate") == 0);
    bool isPersist = (std::strcmp(FLAGS_journal_mode, "PERSIST") == 0 || 
                       std::strcmp(FLAGS_journal_mode, "persist") == 0);
    bool isMemory = (std::strcmp(FLAGS_journal_mode, "MEMORY") == 0 || 
                     std::strcmp(FLAGS_journal_mode, "memory") == 0);
    bool isWal = (std::strcmp(FLAGS_journal_mode, "WAL") == 0 || 
                  std::strcmp(FLAGS_journal_mode, "wal") == 0);
    bool isOff = (std::strcmp(FLAGS_journal_mode, "OFF") == 0 || 
                  std::strcmp(FLAGS_journal_mode, "off") == 0);

    if (!(isDelete || isTrunc || isPersist || isMemory || isWal || isOff)) {
      fprintf(stderr, "No matching journal mode\n");
      return -2;
    }

    char exec[] = "PRAGMA journal_mode = ";
    char* journal_exec = new char[std::strlen(exec)+std::strlen(FLAGS_journal_mode)+1];

    std::strcpy(journal_exec, exec);
    std::strcat(journal_exec, FLAGS_journal_mode);

    char* err_msg = NULL;
    int status;
    return sqlite3_exec(db_, journal_exec, NULL, NULL, &err_msg);
  }

  // #2. Write a code for setting page size in the SQLite database engine
  // [Requirement]
  // (1) This function set page size using "FLAGS_page_size" extern variable (user input)
  // (2) This function return status (int data type) of sqltie API function
  // (3) This function is called at benchmark_open() in bench.cc
  int Benchmark::benchmark_setPageSize() {
    char page_exec[100];
    char* err_msg = NULL;
    snprintf(page_exec, sizeof(page_exec), "PRAGMA page_size = %d", 
              FLAGS_page_size);
    fprintf(stderr, "Page size:    %d bytes\n", FLAGS_page_size);
    return sqlite3_exec(db_, page_exec, NULL, NULL, &err_msg);
  }

  // #3. Write a code for insert function (direct SQL execution mode)
  // [Requriement]
  // (1) This function fill random key-value data using direct qurey execution sqlite API function
  //     (please refer to lecture slide project 3)
  // (2) This function has single arguemnt num_ which is total number of operations
  // (3) This function create SQL statement (key-value pair) do not modfiy this code 
  // (4) This function execute given SQL statement
  // (5) This function is called at benchmark_open() in bench.cc
  void Benchmark::benchmark_directFillRand(int num_) {
    for (int i = 0; i < num_; i++) {
      //      DO NOT MODIFY HERE     //
      const char* value = gen_.rand_gen_generate(FLAGS_value_size);
      char key[100];
      const int k = gen_.rand_next() % num_;

      snprintf(key, sizeof(key), "%016d", k);
      char fill_stmt[100];
      snprintf(fill_stmt, sizeof(fill_stmt), "INSERT INTO test values (%s , x'%x')", key ,value);
      ////////////////////////////////
    
      char* err_msg = NULL;
      int status;
      status = sqlite3_exec(db_, fill_stmt, NULL, NULL, &err_msg);
      error_check(status);
    }
  }

  // xxx(homework)
  // This function execute random write operations to the same key total num_ times one by one
  // in order to implement skewed workloads where a large volume or writes apply to the same key.
  void Benchmark::benchmark_skewedWrite(int num_) {

    benchmark_write(false, SKEWED, FRESH, num_, FLAGS_value_size, 1);
  }

  // This function execute batch write operations to the same key total num_ times
  // in order to implement skewed workloads where a large volume or writes apply to the same key.
  void Benchmark::benchmark_skewedWriteBatch(int num_) {

    benchmark_write(false, SKEWED, FRESH, num_, FLAGS_value_size, 1000);
  }
  
  // This function execute read operations to the same key in case of skewed read workload.
  void Benchmark::benchmark_skewedRead() {
    benchmark_read(SKEWED, 1);
  }

  // This function execute read operations to the same key in case of skewed read workload.
  void Benchmark::benchmark_skewedReadBatch() {
    benchmark_read(RANDOM, 1000);
  }

  // This function execute read operations to sequential keys for sequential read workload.
  void Benchmark::benchmark_seqRead() {
    benchmark_read(SEQUENTIAL, 1);
  }

  // This function execute batch read operations to sequential keys for sequential read workload.
  void Benchmark::benchmark_seqReadBatch() {
    benchmark_read(SEQUENTIAL, 1000);
  }

  // This function execute read operations to random keys for random read workload.
  void Benchmark::benchmark_randRead() {
    benchmark_read(RANDOM, 1);
  }

  // This function execute batch read operations to random keys for random read workload.
  void Benchmark::benchmark_randReadBatch() {
    benchmark_read(RANDOM, 1000);
  }

}; // namespace sqliteBench

