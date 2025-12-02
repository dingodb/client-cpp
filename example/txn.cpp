// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#include <iostream>
#include <string>
#include <tikv/tikv_client.h>

extern "C" int commit_callback(int code, void *ctx) {
  std::cout << "commit result code: " << code << std::endl;
  return 0; // 返回值未使用，仅占位
}

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

  txn.commit(commit_callback, nullptr);
}

void clean_all_data(tikv_client::TransactionClient &client) {
  auto txn = client.begin();

  auto keys =
      txn.scan_keys("", Bound::Included, "", Bound::Unbounded, UINT32_MAX);
  std::cout << "found " << keys.size() << " keys to delete." << std::endl;
  for (auto &key : keys) {
    txn.remove(key);
  }

  txn.commit(commit_callback, nullptr);
}

int main() {
  auto client = tikv_client::TransactionClient({"10.220.32.40:2379"});
  test01(client);

  return 0;
}
