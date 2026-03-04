// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use core::panic;
use std::ops;
use std::sync::Arc;

use anyhow::Result;
use cxx::{CxxString, CxxVector};
use tikv_client::TimestampExt;
use tokio::sync::Mutex;
use tokio::{
    runtime::Runtime,
    time::{timeout, Duration},
};

use self::ffi::*;

#[cxx::bridge]
mod ffi {
    struct Key {
        key: Vec<u8>,
    }

    #[namespace = "ffi"]
    struct KvPair {
        key: Vec<u8>,
        value: Vec<u8>,
    }

    struct OptionalValue {
        is_none: bool,
        value: Vec<u8>,
    }

    enum Bound {
        Included,
        Excluded,
        Unbounded,
    }

    struct TxnOptions {
        try_one_pc: bool,
        async_commit: bool,
        read_only: bool,
    }

    #[namespace = "tikv_client_glue"]
    extern "Rust" {
        type TransactionClient;
        type Transaction;
        type RawKVClient;

        fn raw_client_new(pd_endpoints: &CxxVector<CxxString>) -> Result<Box<RawKVClient>>;

        fn raw_get(client: &RawKVClient, key: &CxxString, timeout_ms: u64)
            -> Result<OptionalValue>;

        fn raw_put(
            cli: &RawKVClient,
            key: &CxxString,
            val: &CxxString,
            timeout_ms: u64,
        ) -> Result<()>;

        fn raw_scan(
            cli: &RawKVClient,
            start: &CxxString,
            end: &CxxString,
            limit: u32,
            timeout_ms: u64,
        ) -> Result<Vec<KvPair>>;

        fn raw_delete(cli: &RawKVClient, key: &CxxString, timeout_ms: u64) -> Result<()>;

        fn raw_delete_range(
            cli: &RawKVClient,
            startKey: &CxxString,
            endKey: &CxxString,
            timeout_ms: u64,
        ) -> Result<()>;

        fn raw_batch_put(
            cli: &RawKVClient,
            pairs: &CxxVector<KvPair>,
            timeout_ms: u64,
        ) -> Result<()>;

        fn transaction_client_new(
            pd_endpoints: &CxxVector<CxxString>,
        ) -> Result<Box<TransactionClient>>;

        fn transaction_client_begin(
            client: &TransactionClient,
            options: TxnOptions,
        ) -> Result<Box<Transaction>>;

        fn transaction_client_begin_pessimistic(
            client: &TransactionClient,
        ) -> Result<Box<Transaction>>;

        fn transaction_timestamp(transaction: &Transaction) -> u64;

        fn transaction_get(transaction: &mut Transaction, key: &CxxString)
            -> Result<OptionalValue>;

        fn transaction_get_for_update(
            transaction: &mut Transaction,
            key: &CxxString,
        ) -> Result<OptionalValue>;

        fn transaction_batch_get(
            transaction: &mut Transaction,
            keys: &CxxVector<CxxString>,
        ) -> Result<Vec<KvPair>>;

        fn transaction_batch_get_for_update(
            transaction: &mut Transaction,
            keys: &CxxVector<CxxString>,
        ) -> Result<Vec<KvPair>>;

        fn transaction_scan(
            transaction: &mut Transaction,
            start: &CxxString,
            start_bound: Bound,
            end: &CxxString,
            end_bound: Bound,
            limit: u32,
        ) -> Result<Vec<KvPair>>;

        fn transaction_scan_keys(
            transaction: &mut Transaction,
            start: &CxxString,
            start_bound: Bound,
            end: &CxxString,
            end_bound: Bound,
            limit: u32,
        ) -> Result<Vec<Key>>;

        fn transaction_put(
            transaction: &mut Transaction,
            key: &CxxString,
            val: &CxxString,
        ) -> Result<()>;

        fn transaction_delete(transaction: &mut Transaction, key: &CxxString) -> Result<()>;

        fn transaction_commit(transaction: &mut Transaction) -> Result<()>;

        fn transaction_rollback(transaction: &mut Transaction) -> Result<()>;

        // Async variants - using function pointer and context
        fn transaction_get_async(
            transaction: &Transaction,
            key: &CxxString,
            callback: usize,
            ctx: usize,
        );

        fn transaction_put_async(
            transaction: &Transaction,
            key: &CxxString,
            val: &CxxString,
            callback: usize,
            ctx: usize,
        );

        fn transaction_delete_async(
            transaction: &Transaction,
            key: &CxxString,
            callback: usize,
            ctx: usize,
        );

        fn transaction_commit_async(
            transaction: &Transaction,
            callback: usize,
            ctx: usize,
        );

        fn transaction_rollback_async(
            transaction: &Transaction,
            callback: usize,
            ctx: usize,
        );

        fn transaction_batch_get_async(
            transaction: &Transaction,
            keys: &CxxVector<CxxString>,
            callback: usize,
            ctx: usize,
        );

        fn transaction_batch_get_for_update_async(
            transaction: &Transaction,
            keys: &CxxVector<CxxString>,
            callback: usize,
            ctx: usize,
        );

        fn transaction_scan_async(
            transaction: &Transaction,
            start: &CxxString,
            start_bound: Bound,
            end: &CxxString,
            end_bound: Bound,
            limit: u32,
            callback: usize,
            ctx: usize,
        );

        fn transaction_scan_keys_async(
            transaction: &Transaction,
            start: &CxxString,
            start_bound: Bound,
            end: &CxxString,
            end_bound: Bound,
            limit: u32,
            callback: usize,
            ctx: usize,
        );

        fn transaction_batch_put_async(
            transaction: &Transaction,
            pairs: &CxxVector<KvPair>,
            callback: usize,
            ctx: usize,
        );
    }
}

struct TransactionClient {
    pub rt: tokio::runtime::Runtime,
    inner: tikv_client::TransactionClient,
}

struct RawKVClient {
    pub rt: tokio::runtime::Runtime,
    inner: tikv_client::RawClient,
}

// Re-architected Transaction to support async operations
// Wrap inner transaction in Arc<Mutex<_>> for thread-safe sharing across async tasks
struct Transaction {
    rt: tokio::runtime::Handle,
    inner: Arc<Mutex<tikv_client::Transaction>>,
}

// Callback type definition for async operations
type AsyncCallback = unsafe extern "C" fn(result: *const u8, result_len: usize, error: *const u8, error_len: usize, ctx: usize);

fn raw_client_new(pd_endpoints: &CxxVector<CxxString>) -> Result<Box<RawKVClient>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    let runtime = Runtime::new().unwrap();

    let pd_endpoints = pd_endpoints
        .iter()
        .map(|str| str.to_str().map(ToOwned::to_owned))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(Box::new(RawKVClient {
        inner: runtime.block_on(tikv_client::RawClient::new(pd_endpoints))?,
        rt: runtime,
    }))
}

fn transaction_client_new(pd_endpoints: &CxxVector<CxxString>) -> Result<Box<TransactionClient>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let runtime = Runtime::new().unwrap();

    let pd_endpoints = pd_endpoints
        .iter()
        .map(|str| str.to_str().map(ToOwned::to_owned))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(Box::new(TransactionClient {
        inner: runtime.block_on(tikv_client::TransactionClient::new(pd_endpoints))?,
        rt: runtime,
    }))
}

fn transaction_client_begin(
    client: &TransactionClient,
    options: TxnOptions,
) -> Result<Box<Transaction>> {
    let mut txn_options = tikv_client::transaction::TransactionOptions::new_optimistic();
    if options.try_one_pc {
        txn_options = txn_options.try_one_pc();
    }
    if options.async_commit {
        txn_options = txn_options.use_async_commit();
    }
    if options.read_only {
        txn_options = txn_options.read_only();
    }

    Ok(Box::new(Transaction {
        rt: client.rt.handle().clone(),
        inner: Arc::new(Mutex::new(client
            .rt
            .block_on(client.inner.begin_with_options(txn_options))?)),
    }))
}

fn transaction_client_begin_pessimistic(client: &TransactionClient) -> Result<Box<Transaction>> {
    Ok(Box::new(Transaction {
        rt: client.rt.handle().clone(),
        inner: Arc::new(Mutex::new(client.rt.block_on(client.inner.begin_pessimistic())?)),
    }))
}

fn raw_get(cli: &RawKVClient, key: &CxxString, timeout_ms: u64) -> Result<OptionalValue> {
    cli.rt.block_on(async {
        let result = timeout(
            Duration::from_millis(timeout_ms),
            cli.inner.get(key.as_bytes().to_vec()),
        )
        .await??;
        match result {
            Some(value) => Ok(OptionalValue {
                is_none: false,
                value,
            }),
            None => Ok(OptionalValue {
                is_none: true,
                value: Vec::new(),
            }),
        }
    })
}

fn raw_put(cli: &RawKVClient, key: &CxxString, val: &CxxString, timeout_ms: u64) -> Result<()> {
    cli.rt.block_on(async {
        timeout(
            Duration::from_millis(timeout_ms),
            cli.inner
                .put(key.as_bytes().to_vec(), val.as_bytes().to_vec()),
        )
        .await??;
        Ok(())
    })
}

fn raw_scan(
    cli: &RawKVClient,
    start: &CxxString,
    end: &CxxString,
    limit: u32,
    timeout_ms: u64,
) -> Result<Vec<KvPair>> {
    cli.rt.block_on(async {
        let rg = to_bound_range(start, Bound::Included, end, Bound::Excluded);
        let result =
            timeout(Duration::from_millis(timeout_ms), cli.inner.scan(rg, limit)).await??;
        let pairs = result
            .into_iter()
            .map(|tikv_client::KvPair(key, value)| KvPair {
                key: key.into(),
                value,
            })
            .collect();
        Ok(pairs)
    })
}

fn raw_delete(cli: &RawKVClient, key: &CxxString, timeout_ms: u64) -> Result<()> {
    cli.rt.block_on(async {
        timeout(
            Duration::from_millis(timeout_ms),
            cli.inner.delete(key.as_bytes().to_vec()),
        )
        .await??;
        Ok(())
    })
}

fn raw_delete_range(
    cli: &RawKVClient,
    start_key: &CxxString,
    end_key: &CxxString,
    timeout_ms: u64,
) -> Result<()> {
    cli.rt.block_on(async {
        let rg = to_bound_range(start_key, Bound::Included, end_key, Bound::Excluded);
        timeout(
            Duration::from_millis(timeout_ms),
            cli.inner.delete_range(rg),
        )
        .await??;
        Ok(())
    })
}

fn raw_batch_put(cli: &RawKVClient, pairs: &CxxVector<KvPair>, timeout_ms: u64) -> Result<()> {
    cli.rt.block_on(async {
        let tikv_pairs: Vec<tikv_client::KvPair> = pairs
            .iter()
            .map(|KvPair { key, value }| -> tikv_client::KvPair {
                tikv_client::KvPair(key.to_vec().into(), value.to_vec())
            })
            .collect();
        timeout(
            Duration::from_millis(timeout_ms),
            cli.inner.batch_put(tikv_pairs),
        )
        .await??;
        Ok(())
    })
}

fn transaction_timestamp(transaction: &Transaction) -> u64 {
    let inner = transaction.inner.blocking_lock();
    inner.start_timestamp().version()
}

fn transaction_get(transaction: &mut Transaction, key: &CxxString) -> Result<OptionalValue> {
    let key = key.as_bytes().to_vec();
    let inner = transaction.inner.clone();
    match transaction
        .rt
        .block_on(async move {
            let mut txn = inner.lock().await;
            txn.get(key).await
        })?
    {
        Some(value) => Ok(OptionalValue {
            is_none: false,
            value,
        }),
        None => Ok(OptionalValue {
            is_none: true,
            value: Vec::new(),
        }),
    }
}

fn transaction_get_for_update(
    transaction: &mut Transaction,
    key: &CxxString,
) -> Result<OptionalValue> {
    let key = key.as_bytes().to_vec();
    let inner = transaction.inner.clone();
    match transaction
        .rt
        .block_on(async move {
            let mut txn = inner.lock().await;
            txn.get_for_update(key).await
        })?
    {
        Some(value) => Ok(OptionalValue {
            is_none: false,
            value,
        }),
        None => Ok(OptionalValue {
            is_none: true,
            value: Vec::new(),
        }),
    }
}

fn transaction_batch_get(
    transaction: &mut Transaction,
    keys: &CxxVector<CxxString>,
) -> Result<Vec<KvPair>> {
    let keys: Vec<Vec<u8>> = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
    let inner = transaction.inner.clone();
    let kv_pairs = transaction
        .rt
        .block_on(async move {
            let mut txn = inner.lock().await;
            txn.batch_get(keys).await
        })?
        .map(|tikv_client::KvPair(key, value)| KvPair {
            key: key.into(),
            value,
        })
        .collect();
    Ok(kv_pairs)
}

fn transaction_batch_get_for_update(
    _transaction: &mut Transaction,
    _keys: &CxxVector<CxxString>,
) -> Result<Vec<KvPair>> {
    unimplemented!("batch_get_for_update is not working properly so far.")
}

fn transaction_scan(
    transaction: &mut Transaction,
    start: &CxxString,
    start_bound: Bound,
    end: &CxxString,
    end_bound: Bound,
    limit: u32,
) -> Result<Vec<KvPair>> {
    let range = to_bound_range(start, start_bound, end, end_bound);
    let inner = transaction.inner.clone();
    let kv_pairs = transaction
        .rt
        .block_on(async move {
            let mut txn = inner.lock().await;
            txn.scan(range, limit).await
        })?
        .map(|tikv_client::KvPair(key, value)| KvPair {
            key: key.into(),
            value,
        })
        .collect();
    Ok(kv_pairs)
}

fn transaction_scan_keys(
    transaction: &mut Transaction,
    start: &CxxString,
    start_bound: Bound,
    end: &CxxString,
    end_bound: Bound,
    limit: u32,
) -> Result<Vec<Key>> {
    let range = to_bound_range(start, start_bound, end, end_bound);
    let inner = transaction.inner.clone();
    let keys = transaction
        .rt
        .block_on(async move {
            let mut txn = inner.lock().await;
            txn.scan_keys(range, limit).await
        })?
        .map(|key| Key { key: key.into() })
        .collect();
    Ok(keys)
}

fn transaction_put(transaction: &mut Transaction, key: &CxxString, val: &CxxString) -> Result<()> {
    let key = key.as_bytes().to_vec();
    let val = val.as_bytes().to_vec();
    let inner = transaction.inner.clone();
    transaction.rt.block_on(async move {
        let mut txn = inner.lock().await;
        txn.put(key, val).await
    })?;
    Ok(())
}

fn transaction_delete(transaction: &mut Transaction, key: &CxxString) -> Result<()> {
    let key = key.as_bytes().to_vec();
    let inner = transaction.inner.clone();
    transaction
        .rt
        .block_on(async move {
            let mut txn = inner.lock().await;
            txn.delete(key).await
        })?;
    Ok(())
}

fn transaction_commit(transaction: &mut Transaction) -> Result<()> {
    let inner = transaction.inner.clone();
    let _timestamp = transaction.rt.block_on(async move {
        let mut txn = inner.lock().await;
        txn.commit().await
    })?;
    Ok(())
}

fn transaction_rollback(transaction: &mut Transaction) -> Result<()> {
    let inner = transaction.inner.clone();
    transaction
        .rt
        .block_on(async move {
            let mut txn = inner.lock().await;
            txn.rollback().await
        })?;
    Ok(())
}

// Helper function to invoke the callback from async context
unsafe fn invoke_callback(
    callback: usize,
    result: Option<Vec<u8>>,
    error: Option<String>,
    ctx: usize,
) {
    let cb: AsyncCallback = std::mem::transmute(callback);

    let (result_ptr, result_len) = match result {
        Some(data) => {
            let len = data.len();
            let ptr = Box::into_raw(data.into_boxed_slice()) as *const u8;
            (ptr, len)
        }
        None => (std::ptr::null(), 0),
    };

    let (error_ptr, error_len) = match &error {
        Some(err) => {
            let err_len = err.len();
            let c_string = std::ffi::CString::new(err.clone())
                .unwrap_or_else(|_| std::ffi::CString::new("Unknown error").unwrap());
            let ptr = c_string.into_raw() as *const u8;
            (ptr, err_len)
        }
        None => (std::ptr::null(), 0),
    };

    cb(result_ptr, result_len, error_ptr, error_len, ctx);
}

// Async implementations using usize for pointer representation

fn transaction_get_async(
    transaction: &Transaction,
    key: &CxxString,
    callback: usize,
    ctx: usize,
) {
    let key = key.as_bytes().to_vec();
    let handle = transaction.rt.clone();
    let inner = transaction.inner.clone();

    handle.spawn(async move {
        let result = async {
            let mut txn = inner.lock().await;
            txn.get(key).await
        }.await;

        match result {
            Ok(value) => {
                let opt_value = value.map(|v| OptionalValue {
                    is_none: false,
                    value: v,
                }).unwrap_or_else(|| OptionalValue {
                    is_none: true,
                    value: Vec::new(),
                });
                // Serialize: is_none (1 byte) + value length (4 bytes) + value
                let mut bytes = Vec::new();
                bytes.push(if opt_value.is_none { 1 } else { 0 });
                let value_len = opt_value.value.len() as u32;
                bytes.extend_from_slice(&value_len.to_le_bytes());
                bytes.extend_from_slice(&opt_value.value);
                unsafe {
                    invoke_callback(callback, Some(bytes), None, ctx);
                }
            }
            Err(e) => {
                unsafe {
                    invoke_callback(callback, None, Some(format!("{}", e)), ctx);
                }
            }
        }
    });
}

fn transaction_put_async(
    transaction: &Transaction,
    key: &CxxString,
    val: &CxxString,
    callback: usize,
    ctx: usize,
) {
    let key = key.as_bytes().to_vec();
    let val = val.as_bytes().to_vec();
    let handle = transaction.rt.clone();
    let inner = transaction.inner.clone();

    handle.spawn(async move {
        let result = async {
            let mut txn = inner.lock().await;
            txn.put(key, val).await
        }.await;

        match result {
            Ok(()) => {
                unsafe {
                    invoke_callback(callback, None, None, ctx);
                }
            }
            Err(e) => {
                unsafe {
                    invoke_callback(callback, None, Some(format!("{}", e)), ctx);
                }
            }
        }
    });
}

fn transaction_delete_async(
    transaction: &Transaction,
    key: &CxxString,
    callback: usize,
    ctx: usize,
) {
    let key = key.as_bytes().to_vec();
    let handle = transaction.rt.clone();
    let inner = transaction.inner.clone();

    handle.spawn(async move {
        let result = async {
            let mut txn = inner.lock().await;
            txn.delete(key).await
        }.await;

        match result {
            Ok(()) => {
                unsafe {
                    invoke_callback(callback, None, None, ctx);
                }
            }
            Err(e) => {
                unsafe {
                    invoke_callback(callback, None, Some(format!("{}", e)), ctx);
                }
            }
        }
    });
}

fn transaction_commit_async(
    transaction: &Transaction,
    callback: usize,
    ctx: usize,
) {
    let handle = transaction.rt.clone();
    let inner = transaction.inner.clone();

    handle.spawn(async move {
        let result = async {
            let mut txn = inner.lock().await;
            txn.commit().await
        }.await;

        match result {
            Ok(_) => {
                unsafe {
                    invoke_callback(callback, None, None, ctx);
                }
            }
            Err(e) => {
                unsafe {
                    invoke_callback(callback, None, Some(format!("{}", e)), ctx);
                }
            }
        }
    });
}

fn transaction_rollback_async(
    transaction: &Transaction,
    callback: usize,
    ctx: usize,
) {
    let handle = transaction.rt.clone();
    let inner = transaction.inner.clone();

    handle.spawn(async move {
        let result = async {
            let mut txn = inner.lock().await;
            txn.rollback().await
        }.await;

        match result {
            Ok(()) => {
                unsafe {
                    invoke_callback(callback, None, None, ctx);
                }
            }
            Err(e) => {
                unsafe {
                    invoke_callback(callback, None, Some(format!("{}", e)), ctx);
                }
            }
        }
    });
}

fn transaction_batch_get_async(
    transaction: &Transaction,
    keys: &CxxVector<CxxString>,
    callback: usize,
    ctx: usize,
) {
    let keys: Vec<Vec<u8>> = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
    let handle = transaction.rt.clone();
    let inner = transaction.inner.clone();

    handle.spawn(async move {
        let result = async {
            let mut txn = inner.lock().await;
            txn.batch_get(keys).await
        }.await;

        match result {
            Ok(kv_iter) => {
                let kv_pairs: Vec<KvPair> = kv_iter
                    .map(|tikv_client::KvPair(key, value)| KvPair {
                        key: key.into(),
                        value,
                    })
                    .collect();
                // Serialize: count (4 bytes) + [key_len (4 bytes) + key + value_len (4 bytes) + value]...
                let mut bytes = Vec::new();
                let count = kv_pairs.len() as u32;
                bytes.extend_from_slice(&count.to_le_bytes());
                for pair in kv_pairs {
                    let key_len = pair.key.len() as u32;
                    let value_len = pair.value.len() as u32;
                    bytes.extend_from_slice(&key_len.to_le_bytes());
                    bytes.extend_from_slice(&pair.key);
                    bytes.extend_from_slice(&value_len.to_le_bytes());
                    bytes.extend_from_slice(&pair.value);
                }
                unsafe {
                    invoke_callback(callback, Some(bytes), None, ctx);
                }
            }
            Err(e) => {
                unsafe {
                    invoke_callback(callback, None, Some(format!("{}", e)), ctx);
                }
            }
        }
    });
}

fn transaction_batch_get_for_update_async(
    _transaction: &Transaction,
    _keys: &CxxVector<CxxString>,
    callback: usize,
    ctx: usize,
) {
    // For now, just return an error since the sync version is unimplemented
    let handle = _transaction.rt.clone();
    handle.spawn(async move {
        unsafe {
            invoke_callback(callback, None, Some("batch_get_for_update is not implemented yet".to_string()), ctx);
        }
    });
}

fn transaction_scan_async(
    transaction: &Transaction,
    start: &CxxString,
    start_bound: Bound,
    end: &CxxString,
    end_bound: Bound,
    limit: u32,
    callback: usize,
    ctx: usize,
) {
    let range = to_bound_range(start, start_bound, end, end_bound);
    let handle = transaction.rt.clone();
    let inner = transaction.inner.clone();

    handle.spawn(async move {
        let result = async {
            let mut txn = inner.lock().await;
            txn.scan(range, limit).await
        }.await;

        match result {
            Ok(kv_iter) => {
                let kv_pairs: Vec<KvPair> = kv_iter
                    .map(|tikv_client::KvPair(key, value)| KvPair {
                        key: key.into(),
                        value,
                    })
                    .collect();
                // Serialize: count (4 bytes) + [key_len (4 bytes) + key + value_len (4 bytes) + value]...
                let mut bytes = Vec::new();
                let count = kv_pairs.len() as u32;
                bytes.extend_from_slice(&count.to_le_bytes());
                for pair in kv_pairs {
                    let key_len = pair.key.len() as u32;
                    let value_len = pair.value.len() as u32;
                    bytes.extend_from_slice(&key_len.to_le_bytes());
                    bytes.extend_from_slice(&pair.key);
                    bytes.extend_from_slice(&value_len.to_le_bytes());
                    bytes.extend_from_slice(&pair.value);
                }
                unsafe {
                    invoke_callback(callback, Some(bytes), None, ctx);
                }
            }
            Err(e) => {
                unsafe {
                    invoke_callback(callback, None, Some(format!("{}", e)), ctx);
                }
            }
        }
    });
}

fn transaction_scan_keys_async(
    transaction: &Transaction,
    start: &CxxString,
    start_bound: Bound,
    end: &CxxString,
    end_bound: Bound,
    limit: u32,
    callback: usize,
    ctx: usize,
) {
    let range = to_bound_range(start, start_bound, end, end_bound);
    let handle = transaction.rt.clone();
    let inner = transaction.inner.clone();

    handle.spawn(async move {
        let result = async {
            let mut txn = inner.lock().await;
            txn.scan_keys(range, limit).await
        }.await;

        match result {
            Ok(key_iter) => {
                let keys: Vec<Key> = key_iter
                    .map(|key| Key { key: key.into() })
                    .collect();
                // Serialize: count (4 bytes) + [key_len (4 bytes) + key]...
                let mut bytes = Vec::new();
                let count = keys.len() as u32;
                bytes.extend_from_slice(&count.to_le_bytes());
                for key in keys {
                    let key_len = key.key.len() as u32;
                    bytes.extend_from_slice(&key_len.to_le_bytes());
                    bytes.extend_from_slice(&key.key);
                }
                unsafe {
                    invoke_callback(callback, Some(bytes), None, ctx);
                }
            }
            Err(e) => {
                unsafe {
                    invoke_callback(callback, None, Some(format!("{}", e)), ctx);
                }
            }
        }
    });
}

fn transaction_batch_put_async(
    transaction: &Transaction,
    pairs: &CxxVector<KvPair>,
    callback: usize,
    ctx: usize,
) {
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = pairs
        .iter()
        .map(|pair| (pair.key.to_vec(), pair.value.to_vec()))
        .collect();
    let handle = transaction.rt.clone();
    let inner = transaction.inner.clone();

    handle.spawn(async move {
        let result = async {
            let mut txn = inner.lock().await;
            for (key, value) in pairs {
                txn.put(key, value).await?;
            }
            Ok::<(), tikv_client::Error>(())
        }.await;

        match result {
            Ok(()) => {
                unsafe {
                    invoke_callback(callback, None, None, ctx);
                }
            }
            Err(e) => {
                unsafe {
                    invoke_callback(callback, None, Some(format!("{}", e)), ctx);
                }
            }
        }
    });
}

fn to_bound_range(
    start: &CxxString,
    start_bound: Bound,
    end: &CxxString,
    end_bound: Bound,
) -> tikv_client::BoundRange {
    let start_bound = match start_bound {
        Bound::Included => ops::Bound::Included(start.as_bytes().to_vec()),
        Bound::Excluded => ops::Bound::Excluded(start.as_bytes().to_vec()),
        Bound::Unbounded => ops::Bound::Unbounded,
        _ => panic!("unexpected bound"),
    };
    let end_bound = match end_bound {
        Bound::Included => ops::Bound::Included(end.as_bytes().to_vec()),
        Bound::Excluded => ops::Bound::Excluded(end.as_bytes().to_vec()),
        Bound::Unbounded => ops::Bound::Unbounded,
        _ => panic!("unexpected bound"),
    };
    tikv_client::BoundRange::from((start_bound, end_bound))
}
