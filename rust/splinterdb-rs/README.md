# `splinterdb-rs`

This crate aims to be a safe and ergonomic Rust wrapper for SplinterDB's public API.

By default, it exposes a simple key/value abstraction, akin to that of the SplinterDB `default_data_config`.

## The `splinterdb-rs` API
#### splinterdb-rs::new::\<T\>()
Returns a new (uninitialized) SplinterDB object `sdb`.  
Here `T` is a struct that implements the `SdbRustDataFuncs` trait, i.e. the rust callbacks for SplinterDB's data_config. If you do not wish to define your own callbacks, use the default callbacks: `splinterdb-rs::new::<DefaultSdb>();`.

#### sdb.db_create(path, cfg)
Create a database or overwrite an existing one at `path` and configure `sdb` using `cfg`.

#### sdb.db_open(path, cfg)
Open an existing database at `path` and configure `sdb` using `cfg`.

#### sdb.insert(key, value)
Insert a new key/value pair or overwrite an existing key/value pair.

#### sdb.update(key, delta)
Perform an update of key using delta. See the rust callbacks for defining the semantics of this update.

#### sdb.delete(key)
Delete a key from the database.

#### sdb.lookup(key)
Lookup the current value of a key.  
Returns a `LookupResult = enum { Found(Vec<u8>),
    FoundTruncated(Vec<u8>),
    NotFound }`.

#### sdb.range(start_key)
Perform a range query beginning at `start_key`.  
This function returns `ri: RangeIterator` from which key/value pairs may be iteratively extracted using `ri.next()`. Thus the range is defined by the `start_key` and the number of calls to `ri.next()`.

## Rust Callbacks for SplinterDB's data_config
To implement merge/update functionality for SplinterDB or, for example, to use a custom `key_compare` function requires the user to implement rust functions for SplinterDB to call.

These functions are defined by the `SdbRustDataFuncs` trait. See `src/rust_cfg.rs` for the definition of the trait, comments on each function, and the default implementations. Define a new set of callbacks as follows:
```
struct Callbacks {}
impl SdbRustDataFuncs for Callbacks
{
    fn key_comp(key1: &[u8], key2: &[u8]) -> CompareResult
    {
        // ...
    }

    fn key_hash(key: &[u8], seed: u32) -> u32
    {
        // ...
    }
    fn merge(_key: &[u8], _old_msg: SdbMessage, new_msg: SdbMessage) -> Result<SdbMessage>
    {
        // ...
    }
    fn merge_final(_key: &[u8], oldest_msg: SdbMessage) -> Result<SdbMessage>
    {
        // ...
    }
    fn str_key(key: &[u8], dst: &mut [u8]) -> ()
    {
        // ...
    }
    fn str_msg(msg: SdbMessage, dst: &mut [u8]) -> ()
    {
        // ...
    }
}
```
Then use these callbacks when creating a new SplinterDB object: `sdb = splinterdb-rs::new::<Callbacks>();`


If any of the default implementations are sufficient for your purposes then those functions may be excluded from your `impl`. See `SimpleMerge` in `src/tests.rs` for an example.