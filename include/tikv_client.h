// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#ifndef _TIKV_CLIENT_H_
#define _TIKV_CLIENT_H_

#include "lib.rs.h"
#include <cstdint>
#include <functional>
#include <future>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

extern "C" {
typedef int (*CallbackWithContext)(int, void *);
}

namespace tikv_client {

struct KvPair final {
  std::string key;
  std::string value;

  KvPair(std::string &&key, std::string &&value);
  ffi::KvPair to_ffi() const;
};

// Callback types for async operations
typedef void (*TransactionGetCallback)(
    const std::optional<std::string> *value,
    const std::string *error,
    void *context
);

typedef void (*TransactionVoidCallback)(
    const std::string *error,  // null on success
    void *context
);

// Callback for batch_get and scan operations (vector of KvPair)
typedef void (*TransactionKvPairsCallback)(
    const std::vector<KvPair> *pairs,
    const std::string *error,
    void *context
);

// Callback for scan_keys operations (vector of keys)
typedef void (*TransactionKeysCallback)(
    const std::vector<std::string> *keys,
    const std::string *error,
    void *context
);

class Transaction {
public:
  Transaction(::rust::cxxbridge1::Box<tikv_client_glue::Transaction> txn);

  uint64_t id() const;

  // Synchronous APIs
  std::optional<std::string> get(const std::string &key);
  std::optional<std::string> get_for_update(const std::string &key);

  std::vector<KvPair> batch_get(const std::vector<std::string> &keys);
  std::vector<KvPair>
  batch_get_for_update(const std::vector<std::string> &keys);

  std::vector<KvPair> scan(const std::string &start, Bound start_bound,
                           const std::string &end, Bound end_bound,
                           std::uint32_t limit);
  std::vector<std::string> scan_keys(const std::string &start,
                                     Bound start_bound, const std::string &end,
                                     Bound end_bound, std::uint32_t limit);

  void put(const std::string &key, const std::string &value);
  void batch_put(const std::vector<KvPair> &kvs);

  void remove(const std::string &key);

  void commit();
  void rollback();

  // Asynchronous APIs
  void get_async(
      const std::string &key,
      TransactionGetCallback callback,
      void *context
  );

  void put_async(
      const std::string &key,
      const std::string &value,
      TransactionVoidCallback callback,
      void *context
  );

  void remove_async(
      const std::string &key,
      TransactionVoidCallback callback,
      void *context
  );

  void commit_async(
      TransactionVoidCallback callback,
      void *context
  );

  void rollback_async(
      TransactionVoidCallback callback,
      void *context
  );

  // Async batch operations
  void batch_get_async(
      const std::vector<std::string> &keys,
      TransactionKvPairsCallback callback,
      void *context
  );

  void batch_get_for_update_async(
      const std::vector<std::string> &keys,
      TransactionKvPairsCallback callback,
      void *context
  );

  void batch_put_async(
      const std::vector<KvPair> &kvs,
      TransactionVoidCallback callback,
      void *context
  );

  // Async scan operations
  void scan_async(
      const std::string &start, Bound start_bound,
      const std::string &end, Bound end_bound,
      std::uint32_t limit,
      TransactionKvPairsCallback callback,
      void *context
  );

  void scan_keys_async(
      const std::string &start, Bound start_bound,
      const std::string &end, Bound end_bound,
      std::uint32_t limit,
      TransactionKeysCallback callback,
      void *context
  );

  // Future-based convenience wrappers
  std::future<std::optional<std::string>> get_async_future(const std::string &key);
  std::future<void> put_async_future(const std::string &key, const std::string &value);
  std::future<void> remove_async_future(const std::string &key);
  std::future<void> commit_async_future();
  std::future<void> rollback_async_future();

  // Future-based wrappers for batch and scan operations
  std::future<std::vector<KvPair>> batch_get_async_future(const std::vector<std::string> &keys);
  std::future<std::vector<KvPair>> batch_get_for_update_async_future(const std::vector<std::string> &keys);
  std::future<void> batch_put_async_future(const std::vector<KvPair> &kvs);

  std::future<std::vector<KvPair>> scan_async_future(
      const std::string &start, Bound start_bound,
      const std::string &end, Bound end_bound,
      std::uint32_t limit);

  std::future<std::vector<std::string>> scan_keys_async_future(
      const std::string &start, Bound start_bound,
      const std::string &end, Bound end_bound,
      std::uint32_t limit);

private:
  ::rust::cxxbridge1::Box<tikv_client_glue::Transaction> _txn;
};

class TransactionClient {
public:
  TransactionClient(const std::vector<std::string> &pd_endpoints);
  Transaction begin(TxnOptions options);

  Transaction begin();
  Transaction begin_pessimistic();

private:
  ::rust::cxxbridge1::Box<tikv_client_glue::TransactionClient> _client;
};

class RawKVClient {
public:
  RawKVClient(const std::vector<std::string> &pd_endpoints);
  std::optional<std::string> get(const std::string &key,
                                 const std::uint64_t timeout);
  void put(const std::string &key, const std::string &value,
           const std::uint64_t timeout);
  void batch_put(const std::vector<KvPair> &kvs, const std::uint64_t timeout);
  void remove(const std::string &key, const std::uint64_t timeout);
  void remove_range(const std::string &start_key, const std::string &end_key,
                    const std::uint64_t timeout);
  std::vector<KvPair> scan(const std::string &startKey,
                           const std::string &endKey, std::uint32_t limit,
                           const std::uint64_t timeout);

private:
  ::rust::cxxbridge1::Box<tikv_client_glue::RawKVClient> _client;
};

} // namespace tikv_client

#endif //_TIKV_CLIENT_H_
