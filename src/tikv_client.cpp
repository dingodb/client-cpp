// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#include "tikv_client.h"

#include <atomic>
#include <cstring>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <unordered_map>

using namespace std;
using ::rust::cxxbridge1::Box;

namespace tikv_client {

KvPair::KvPair(std::string &&key, std::string &&value)
    : key(std::move(key)), value(std::move(value)) {}

ffi::KvPair KvPair::to_ffi() {
  ffi::KvPair f_pair;
  f_pair.key.reserve(key.size());
  for (const auto &c : this->key) {
    f_pair.key.emplace_back(static_cast<std::uint8_t>(c));
  }
  f_pair.value.reserve(value.size());
  for (const auto &c : this->value) {
    f_pair.value.emplace_back(static_cast<std::uint8_t>(c));
  }
  return f_pair;
}

TransactionClient::TransactionClient(
    const std::vector<std::string> &pd_endpoints)
    : _client(tikv_client_glue::transaction_client_new(pd_endpoints)) {}

RawKVClient::RawKVClient(const std::vector<std::string> &pd_endpoints)
    : _client(tikv_client_glue::raw_client_new(pd_endpoints)) {}

std::optional<std::string> RawKVClient::get(const std::string &key,
                                            const std::uint64_t timeout) {
  auto val = tikv_client_glue::raw_get(*_client, key, timeout);
  if (val.is_none) {
    return std::nullopt;
  } else {
    return std::string{val.value.begin(), val.value.end()};
  }
}

void RawKVClient::put(const std::string &key, const std::string &value,
                      const std::uint64_t timeout) {
  tikv_client_glue::raw_put(*_client, key, value, timeout);
}

void RawKVClient::batch_put(const std::vector<KvPair> &kv_pairs,
                            const std::uint64_t timeout) {
  std::vector<ffi::KvPair> pairs;
  pairs.reserve(kv_pairs.size());
  for (auto pair : kv_pairs) {
    pairs.emplace_back(pair.to_ffi());
  }
  tikv_client_glue::raw_batch_put(*_client, pairs, timeout);
}

std::vector<KvPair> RawKVClient::scan(const std::string &startKey,
                                      const std::string &endKey,
                                      std::uint32_t limit,
                                      const std::uint64_t timeout) {
  auto kv_pairs =
      tikv_client_glue::raw_scan(*_client, startKey, endKey, limit, timeout);
  std::vector<KvPair> result;
  result.reserve(kv_pairs.size());
  for (auto iter = kv_pairs.begin(); iter != kv_pairs.end(); ++iter) {
    result.emplace_back(
        std::string{(iter->key).begin(), (iter->key).end()},
        std::string{(iter->value).begin(), (iter->value).end()});
  }
  return result;
}

void RawKVClient::remove(const std::string &key, const std::uint64_t timeout) {
  tikv_client_glue::raw_delete(*_client, key, timeout);
}

void RawKVClient::remove_range(const std::string &start_key,
                               const std::string &end_key,
                               const std::uint64_t timeout) {
  tikv_client_glue::raw_delete_range(*_client, start_key, end_key, timeout);
}

Transaction TransactionClient::begin(TxnOptions options) {
  return Transaction(transaction_client_begin(*_client, options));
}

Transaction TransactionClient::begin() {
  TxnOptions options = {
      .try_one_pc = true, .async_commit = true, .read_only = false};
  return Transaction(transaction_client_begin(*_client, options));
}

Transaction TransactionClient::begin_pessimistic() {
  return Transaction(transaction_client_begin_pessimistic(*_client));
}

Transaction::Transaction(Box<tikv_client_glue::Transaction> txn)
    : _txn(std::move(txn)) {}

uint64_t Transaction::id() const { return transaction_timestamp(*_txn); }

std::optional<std::string> Transaction::get(const std::string &key) {
  auto val = transaction_get(*_txn, key);
  if (val.is_none) {
    return std::nullopt;
  } else {
    return std::string{val.value.begin(), val.value.end()};
  }
}

std::optional<std::string> Transaction::get_for_update(const std::string &key) {
  auto val = transaction_get_for_update(*_txn, key);
  if (val.is_none) {
    return std::nullopt;
  } else {
    return std::string{val.value.begin(), val.value.end()};
  }
}

std::vector<KvPair>
Transaction::batch_get(const std::vector<std::string> &keys) {
  auto kv_pairs = transaction_batch_get(*_txn, keys);
  std::vector<KvPair> result;
  result.reserve(kv_pairs.size());
  for (auto iter = kv_pairs.begin(); iter != kv_pairs.end(); ++iter) {
    result.emplace_back(
        std::string{(iter->key).begin(), (iter->key).end()},
        std::string{(iter->value).begin(), (iter->value).end()});
  }
  return result;
}

std::vector<KvPair>
Transaction::batch_get_for_update(const std::vector<std::string> &keys) {
  auto kv_pairs = transaction_batch_get_for_update(*_txn, keys);
  std::vector<KvPair> result;
  result.reserve(kv_pairs.size());
  for (auto iter = kv_pairs.begin(); iter != kv_pairs.end(); ++iter) {
    result.emplace_back(
        std::string{(iter->key).begin(), (iter->key).end()},
        std::string{(iter->value).begin(), (iter->value).end()});
  }
  return result;
}

std::vector<KvPair> Transaction::scan(const std::string &start,
                                      Bound start_bound, const std::string &end,
                                      Bound end_bound, std::uint32_t limit) {
  auto kv_pairs =
      transaction_scan(*_txn, start, start_bound, end, end_bound, limit);
  std::vector<KvPair> result;
  result.reserve(kv_pairs.size());
  for (auto iter = kv_pairs.begin(); iter != kv_pairs.end(); ++iter) {
    result.emplace_back(
        std::string{(iter->key).begin(), (iter->key).end()},
        std::string{(iter->value).begin(), (iter->value).end()});
  }
  return result;
}

std::vector<std::string> Transaction::scan_keys(const std::string &start,
                                                Bound start_bound,
                                                const std::string &end,
                                                Bound end_bound,
                                                std::uint32_t limit) {
  auto keys =
      transaction_scan_keys(*_txn, start, start_bound, end, end_bound, limit);
  std::vector<std::string> result;
  result.reserve(keys.size());
  for (auto iter = keys.begin(); iter != keys.end(); ++iter) {
    result.emplace_back(std::string{(iter->key).begin(), (iter->key).end()});
  }
  return result;
}

void Transaction::put(const std::string &key, const std::string &value) {
  transaction_put(*_txn, key, value);
}

void Transaction::batch_put(const std::vector<KvPair> &kvs) {
  for (auto iter = kvs.begin(); iter != kvs.end(); ++iter) {
    transaction_put(*_txn, iter->key, iter->value);
  }
}

void Transaction::remove(const std::string &key) {
  transaction_delete(*_txn, key);
}

void Transaction::commit() { transaction_commit(*_txn); }

void Transaction::rollback() { transaction_rollback(*_txn); }

// Async implementation helpers
namespace {

// C-compatible callback signature that matches Rust definition
using RustAsyncCallback = void(*)(const uint8_t* result, size_t result_len,
                                  const uint8_t* error, size_t error_len,
                                  uintptr_t ctx);

// Context structure for async get operations
struct GetAsyncContext {
  TransactionGetCallback callback;
  void* user_context;

  GetAsyncContext(TransactionGetCallback cb, void* ctx)
      : callback(cb), user_context(ctx) {}
};

// Context structure for void async operations
struct VoidAsyncContext {
  TransactionVoidCallback callback;
  void* user_context;

  VoidAsyncContext(TransactionVoidCallback cb, void* ctx)
      : callback(cb), user_context(ctx) {}
};

// Rust-compatible callback adapter for get operations
extern "C" void get_async_rust_callback(
    const uint8_t* result,
    size_t result_len,
    const uint8_t* error,
    size_t error_len,
    uintptr_t ctx) {
  auto* context = reinterpret_cast<GetAsyncContext*>(ctx);

  if (error && error_len > 0) {
    std::string error_msg(reinterpret_cast<const char*>(error), error_len);
    if (context->callback) {
      context->callback(nullptr, &error_msg, context->user_context);
    }
  } else if (result && result_len > 0) {
    // Parse serialized OptionalValue: is_none (1 byte) + value_len (4 bytes) + value
    bool is_none = result[0] != 0;
    std::optional<std::string> value_opt;
    if (!is_none && result_len >= 5) {
      uint32_t value_len = *reinterpret_cast<const uint32_t*>(result + 1);
      if (result_len >= 5 + value_len) {
        value_opt = std::string(reinterpret_cast<const char*>(result + 5), value_len);
      }
    }
    if (context->callback) {
      context->callback(&value_opt, nullptr, context->user_context);
    }
  } else {
    std::optional<std::string> null_opt;
    if (context->callback) {
      context->callback(&null_opt, nullptr, context->user_context);
    }
  }

  delete context;

  // Free the allocated memory from Rust side
  if (result) {
    delete[] result;
  }
  if (error) {
    delete[] error;
  }
}

// Rust-compatible callback adapter for void operations
extern "C" void void_async_rust_callback(
    const uint8_t* /*result*/,
    size_t /*result_len*/,
    const uint8_t* error,
    size_t error_len,
    uintptr_t ctx) {
  auto* context = reinterpret_cast<VoidAsyncContext*>(ctx);

  if (error && error_len > 0) {
    std::string error_msg(reinterpret_cast<const char*>(error), error_len);
    if (context->callback) {
      context->callback(&error_msg, context->user_context);
    }
  } else {
    if (context->callback) {
      context->callback(nullptr, context->user_context);
    }
  }

  delete context;

  // Free the allocated memory from Rust side
  if (error) {
    delete[] error;
  }
}

} // anonymous namespace

// Async implementations
void Transaction::get_async(
    const std::string &key,
    TransactionGetCallback callback,
    void *context) {
  auto* ctx = new GetAsyncContext(callback, context);
  tikv_client_glue::transaction_get_async(
      *_txn,
      key,
      reinterpret_cast<uintptr_t>(get_async_rust_callback),
      reinterpret_cast<uintptr_t>(ctx));
}

void Transaction::put_async(
    const std::string &key,
    const std::string &value,
    TransactionVoidCallback callback,
    void *context) {
  auto* ctx = new VoidAsyncContext(callback, context);
  tikv_client_glue::transaction_put_async(
      *_txn,
      key,
      value,
      reinterpret_cast<uintptr_t>(void_async_rust_callback),
      reinterpret_cast<uintptr_t>(ctx));
}

void Transaction::remove_async(
    const std::string &key,
    TransactionVoidCallback callback,
    void *context) {
  auto* ctx = new VoidAsyncContext(callback, context);
  tikv_client_glue::transaction_delete_async(
      *_txn,
      key,
      reinterpret_cast<uintptr_t>(void_async_rust_callback),
      reinterpret_cast<uintptr_t>(ctx));
}

void Transaction::commit_async(
    TransactionVoidCallback callback,
    void *context) {
  auto* ctx = new VoidAsyncContext(callback, context);
  tikv_client_glue::transaction_commit_async(
      *_txn,
      reinterpret_cast<uintptr_t>(void_async_rust_callback),
      reinterpret_cast<uintptr_t>(ctx));
}

void Transaction::rollback_async(
    TransactionVoidCallback callback,
    void *context) {
  auto* ctx = new VoidAsyncContext(callback, context);
  tikv_client_glue::transaction_rollback_async(
      *_txn,
      reinterpret_cast<uintptr_t>(void_async_rust_callback),
      reinterpret_cast<uintptr_t>(ctx));
}

// Future-based implementations
namespace {
  // Helper to generate unique IDs for promises
  std::atomic<uint64_t> g_promise_id{0};

  template<typename T>
  struct PromiseMap {
    std::mutex mutex;
    std::unordered_map<uint64_t, std::shared_ptr<std::promise<T>>> promises;

    uint64_t add(std::shared_ptr<std::promise<T>> promise) {
      uint64_t id = ++g_promise_id;
      std::lock_guard<std::mutex> lock(mutex);
      promises[id] = std::move(promise);
      return id;
    }

    std::shared_ptr<std::promise<T>> remove(uint64_t id) {
      std::lock_guard<std::mutex> lock(mutex);
      auto it = promises.find(id);
      if (it != promises.end()) {
        auto result = std::move(it->second);
        promises.erase(it);
        return result;
      }
      return nullptr;
    }
  };

  PromiseMap<std::optional<std::string>> g_get_promises;
  PromiseMap<void> g_void_promises;

  extern "C" void get_promise_callback(const std::optional<std::string> *value, const std::string *error, void *ctx) {
    uint64_t id = reinterpret_cast<uintptr_t>(ctx);
    auto promise = g_get_promises.remove(id);
    if (promise) {
      if (error) {
        promise->set_exception(std::make_exception_ptr(std::runtime_error(*error)));
      } else if (value) {
        promise->set_value(*value);
      } else {
        promise->set_value(std::nullopt);
      }
    }
  }

  extern "C" void void_promise_callback(const std::string *error, void *ctx) {
    uint64_t id = reinterpret_cast<uintptr_t>(ctx);
    auto promise = g_void_promises.remove(id);
    if (promise) {
      if (error) {
        promise->set_exception(std::make_exception_ptr(std::runtime_error(*error)));
      } else {
        promise->set_value();
      }
    }
  }
}

std::future<std::optional<std::string>> Transaction::get_async_future(const std::string &key) {
  auto promise = std::make_shared<std::promise<std::optional<std::string>>>();
  auto future = promise->get_future();
  uint64_t id = g_get_promises.add(promise);

  get_async(key, get_promise_callback, reinterpret_cast<void*>(id));

  return future;
}

std::future<void> Transaction::put_async_future(const std::string &key, const std::string &value) {
  auto promise = std::make_shared<std::promise<void>>();
  auto future = promise->get_future();
  uint64_t id = g_void_promises.add(promise);

  put_async(key, value, void_promise_callback, reinterpret_cast<void*>(id));

  return future;
}

std::future<void> Transaction::remove_async_future(const std::string &key) {
  auto promise = std::make_shared<std::promise<void>>();
  auto future = promise->get_future();
  uint64_t id = g_void_promises.add(promise);

  remove_async(key, void_promise_callback, reinterpret_cast<void*>(id));

  return future;
}

std::future<void> Transaction::commit_async_future() {
  auto promise = std::make_shared<std::promise<void>>();
  auto future = promise->get_future();
  uint64_t id = g_void_promises.add(promise);

  commit_async(void_promise_callback, reinterpret_cast<void*>(id));

  return future;
}

std::future<void> Transaction::rollback_async_future() {
  auto promise = std::make_shared<std::promise<void>>();
  auto future = promise->get_future();
  uint64_t id = g_void_promises.add(promise);

  rollback_async(void_promise_callback, reinterpret_cast<void*>(id));

  return future;
}

} // namespace tikv_client
