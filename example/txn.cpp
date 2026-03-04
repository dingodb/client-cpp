// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#include <chrono>
#include <iostream>
#include <string>
#include <thread>
// Include local header instead of installed version
#include "tikv_client.h"

void test01(tikv_client::TransactionClient &client) {
  auto txn = client.begin();

  txn.put("k1", "v2");

  auto val = txn.get("k1");
  if (val) {
    std::cout << "get key k1:" << *val << std::endl;
  } else {
    std::cout << "key not found" << std::endl;
  }

  auto kv_pairs = txn.scan("k1", Bound::Included, "", Bound::Unbounded, 10);
  for (auto iter = kv_pairs.begin(); iter != kv_pairs.end(); ++iter) {
    std::cout << "scan:" << iter->key << ": " << iter->value << std::endl;
  }

  txn.commit();
}

void clean_all_data(tikv_client::TransactionClient &client) {
  auto txn = client.begin();

  auto keys =
      txn.scan_keys("", Bound::Included, "", Bound::Unbounded, UINT32_MAX);
  std::cout << "found " << keys.size() << " keys to delete." << std::endl;
  for (auto &key : keys) {
    txn.remove(key);
  }

  txn.commit();
}

// Async callback example using callback-based API
void test_async_callback(tikv_client::TransactionClient &client) {
  std::cout << "\n=== Test Async Callback API ===" << std::endl;

  auto txn = client.begin();

  // Put key asynchronously
  txn.put_async(
      "async_key1", "async_value1",
      [](const std::string *error, void *ctx) {
        (void)ctx;
        if (error) {
          std::cerr << "Put failed: " << *error << std::endl;
        } else {
          std::cout << "Put async_key1 success" << std::endl;
        }
      },
      nullptr);

  // Get key asynchronously
  txn.get_async(
      "async_key1",
      [](const std::optional<std::string> *value, const std::string *error,
         void *ctx) {
        (void)ctx;
        if (error) {
          std::cerr << "Get failed: " << *error << std::endl;
        } else if (value && *value) {
          std::cout << "Got async_key1: " << **value << std::endl;
        } else {
          std::cout << "async_key1 not found" << std::endl;
        }
      },
      nullptr);

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Commit asynchronously
  txn.commit_async(
      [](const std::string *error, void *ctx) {
        (void)ctx;
        if (error) {
          std::cerr << "Commit failed: " << *error << std::endl;
        } else {
          std::cout << "Commit success" << std::endl;
        }
      },
      nullptr);

  // Wait a bit for async operations to complete
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

// Async example using future-based API
void test_async_future(tikv_client::TransactionClient &client) {
  std::cout << "\n=== Test Async Future API ===" << std::endl;

  auto txn = client.begin();

  // Put key asynchronously and wait for result
  auto put_future = txn.put_async_future("async_key2", "async_value2");
  put_future.wait();
  std::cout << "Put async_key2 success" << std::endl;

  // Get key asynchronously and wait for result
  auto get_future = txn.get_async_future("async_key2");
  auto val = get_future.get();
  if (val) {
    std::cout << "Got async_key2: " << *val << std::endl;
  } else {
    std::cout << "async_key2 not found" << std::endl;
  }

  // Commit asynchronously and wait for result
  auto commit_future = txn.commit_async_future();
  commit_future.wait();
  std::cout << "Commit success" << std::endl;
}

// Example of async remove operation
void test_async_remove(tikv_client::TransactionClient &client) {
  std::cout << "\n=== Test Async Remove ===" << std::endl;

  auto txn = client.begin();

  // First put a key
  txn.put("delete_me", "value");
  txn.commit();

  // New transaction to delete
  auto txn2 = client.begin();

  // Remove asynchronously
  auto remove_future = txn2.remove_async_future("delete_me");
  remove_future.wait();
  std::cout << "Remove success" << std::endl;

  // Commit asynchronously
  auto commit_future = txn2.commit_async_future();
  commit_future.wait();
  std::cout << "Commit success" << std::endl;
}

// Example of rollback async
void test_async_rollback(tikv_client::TransactionClient &client) {
  std::cout << "\n=== Test Async Rollback ===" << std::endl;

  auto txn = client.begin();

  // Put some data
  txn.put("rollback_key", "rollback_value");

  // Rollback asynchronously
  auto rollback_future = txn.rollback_async_future();
  rollback_future.wait();
  std::cout << "Rollback success" << std::endl;
}

// Example of async batch operations using future-based API
void test_async_batch_future(tikv_client::TransactionClient &client) {
  std::cout << "\n=== Test Async Batch Operations (Future API) ===" << std::endl;

  auto txn = client.begin();

  // Prepare batch data
  std::vector<tikv_client::KvPair> kvs;
  kvs.emplace_back(tikv_client::KvPair(std::string("batch_key1"), std::string("batch_value1")));
  kvs.emplace_back(tikv_client::KvPair(std::string("batch_key2"), std::string("batch_value2")));
  kvs.emplace_back(tikv_client::KvPair(std::string("batch_key3"), std::string("batch_value3")));

  // Batch put asynchronously
  auto batch_put_future = txn.batch_put_async_future(kvs);
  batch_put_future.wait();
  std::cout << "Batch put success" << std::endl;

  // Batch get asynchronously
  std::vector<std::string> keys = {"batch_key1", "batch_key2", "batch_key3", "non_existent_key"};
  auto batch_get_future = txn.batch_get_async_future(keys);
  auto results = batch_get_future.get();
  std::cout << "Batch get returned " << results.size() << " results:" << std::endl;
  for (const auto &kv : results) {
    std::cout << "  " << kv.key << ": " << kv.value << std::endl;
  }

  // Commit asynchronously
  auto commit_future = txn.commit_async_future();
  commit_future.wait();
  std::cout << "Commit success" << std::endl;
}

// Example of async scan operations using callback-based API
void test_async_scan_callback(tikv_client::TransactionClient &client) {
  std::cout << "\n=== Test Async Scan Operations (Callback API) ===" << std::endl;

  auto txn = client.begin();

  // First, put some data
  txn.put("scan_key_a", "value_a");
  txn.put("scan_key_b", "value_b");
  txn.put("scan_key_c", "value_c");
  txn.commit();

  // New transaction for scan
  auto txn2 = client.begin();

  // Scan asynchronously with callback
  txn2.scan_async("scan_key_a", Bound::Included, "scan_key_d", Bound::Excluded, 10,
    [](const std::vector<tikv_client::KvPair> *pairs, const std::string *error, void *ctx) {
      (void)ctx;
      if (error) {
        std::cerr << "Scan failed: " << *error << std::endl;
      } else if (pairs) {
        std::cout << "Scan returned " << pairs->size() << " results:" << std::endl;
        for (const auto &kv : *pairs) {
          std::cout << "  " << kv.key << ": " << kv.value << std::endl;
        }
      }
    }, nullptr);

  // Scan keys asynchronously with callback
  txn2.scan_keys_async("scan_key_a", Bound::Included, "scan_key_d", Bound::Excluded, 10,
    [](const std::vector<std::string> *keys, const std::string *error, void *ctx) {
      (void)ctx;
      if (error) {
        std::cerr << "Scan keys failed: " << *error << std::endl;
      } else if (keys) {
        std::cout << "Scan keys returned " << keys->size() << " keys:" << std::endl;
        for (const auto &key : *keys) {
          std::cout << "  " << key << std::endl;
        }
      }
    }, nullptr);

  // Wait for async operations
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Commit
  txn2.commit_async(
    [](const std::string *error, void *ctx) {
      (void)ctx;
      if (error) {
        std::cerr << "Commit failed: " << *error << std::endl;
      } else {
        std::cout << "Commit success" << std::endl;
      }
    }, nullptr);

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

// Example of async scan operations using future-based API
void test_async_scan_future(tikv_client::TransactionClient &client) {
  std::cout << "\n=== Test Async Scan Operations (Future API) ===" << std::endl;

  auto txn = client.begin();

  // Scan asynchronously using future
  auto scan_future = txn.scan_async_future("", Bound::Included, "", Bound::Unbounded, 100);
  auto results = scan_future.get();
  std::cout << "Scan returned " << results.size() << " results" << std::endl;

  // Scan keys asynchronously using future
  auto scan_keys_future = txn.scan_keys_async_future("", Bound::Included, "", Bound::Unbounded, 100);
  auto keys = scan_keys_future.get();
  std::cout << "Scan keys returned " << keys.size() << " keys" << std::endl;

  txn.rollback();
}

int main() {
  auto client = tikv_client::TransactionClient({"10.220.32.40:2379"});
  // test01(client);

  // Test async callback-based API
  test_async_callback(client);

  // Test async future-based API
  // test_async_future(client);

  // Test async remove
  // test_async_remove(client);

  // Test async rollback
  // test_async_rollback(client);

  std::cout << "\nAll tests completed!" << std::endl;
  return 0;
}
